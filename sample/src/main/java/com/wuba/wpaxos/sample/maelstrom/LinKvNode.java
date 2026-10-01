/*
 * Copyright (C) 2005-present, 58.com.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wuba.wpaxos.sample.maelstrom;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wuba.wpaxos.ProposeResult;
import com.wuba.wpaxos.comm.GroupSMInfo;
import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.comm.enums.IndexType;
import com.wuba.wpaxos.config.PaxosTryCommitRet;
import com.wuba.wpaxos.node.Node;
import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

/**
 * A WPaxos node that speaks Maelstrom's stdio JSON-lines protocol and serves the
 * {@code lin-kv} workload.
 *
 * <p>Launched by Maelstrom with no arguments, one process per node, all on the same
 * host. The dialect is the one documented at the pinned Maelstrom commit
 * (480a8197): JSON objects on stdin/stdout, one per line, nothing else on stdout.
 * See {@code maelstrom/doc/protocol.md} and {@code maelstrom/doc/workloads.md} for
 * the {@code lin-kv} RPC shapes, and {@code maelstrom/src/maelstrom/client.clj} for
 * how the workload's client interprets replies.
 *
 * <h2>Key to paxos group mapping</h2>
 *
 * One group per key is the obvious design and is what this harness does not do,
 * because Maelstrom's {@code lin-kv} key set is not knowable in advance -- the
 * workload is defined over an open range of keys, not the five or so a fixed
 * topology would suggest, so a group count fixed at startup cannot be "one group
 * per key" in any meaningful sense.
 *
 * <p>Instead every key hashes to one of a small, fixed number of groups:
 * {@code group = floorMod(key.hashCode(), groupCount)}, with {@code groupCount}
 * defaulting to the number of nodes. The properties that matter:
 *
 * <ul>
 *   <li><b>Deterministic and node-independent.</b> {@code String.hashCode} is
 *       specified by the JLS, so every node and every restart computes the same
 *       group for a key. No coordination, no allocation, no rebalancing.</li>
 *   <li><b>All operations on a key land in one group</b>, hence in one totally
 *       ordered log. That is the requirement: a key's operations are serialised
 *       against each other, which is what makes its history linearizable.</li>
 *   <li><b>Distinct keys may share a group.</b> That is only a throughput effect --
 *       those keys are ordered against each other unnecessarily -- never a
 *       correctness one. The lin-kv checker validates each key independently
 *       ({@code independent/checker} over {@code cas-register}), so no checker
 *       verdict depends on cross-key ordering being tight.</li>
 * </ul>
 *
 * <p>Keeping the group count at or below the node count is deliberate: Maelstrom
 * runs every node on one host, so every group is an independent Paxos instance
 * competing for the same CPUs, and each extra group costs local latency without
 * buying a stronger test.
 */
public class LinKvNode {

	private static final Logger logger = LogManager.getLogger(LinKvNode.class);

	/**
	 * Propose timeout, in milliseconds.
	 *
	 * <p>Deliberately shorter than Maelstrom's client-side timeout for the workload,
	 * which is {@code max(10 * mean-latency, 1000)}. If our propose has not resolved
	 * by the time the client stops waiting, we would be answering a question nobody
	 * is listening to; more to the point, we must not be the reason a request
	 * appears to have succeeded at a moment when consensus has not in fact agreed.
	 */
	private static final int PROPOSE_TIMEOUT_MS = 800;

	/** Maelstrom error codes. See {@code maelstrom/doc/protocol.md}. */
	private static final int ERR_TEMPORARILY_UNAVAILABLE = 11;
	private static final int ERR_CRASH = 13;
	private static final int ERR_KEY_DOES_NOT_EXIST = 20;

	private final PrintStream out;
	private final Object writeLock = new Object();
	private final AtomicLong nextMsgId = new AtomicLong(1);

	private volatile String nodeId;
	private List<String> nodeIds;
	private volatile MaelstromNetwork network;
	private volatile Node paxosNode;
	private volatile int groupCount;
	private String dataRoot;

	private volatile LinKvStateMachine[] machines;
	private final ExecutorService workers;

	public LinKvNode(PrintStream out) {
		this.out = out;
		this.workers = Executors.newCachedThreadPool(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "lin-kv-" + nodeId);
				t.setDaemon(true);
				return t;
			}
		});
	}

	public void run() throws IOException {
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
		String line;
		while ((line = in.readLine()) != null) {
			if (line.trim().isEmpty()) {
				continue;
			}
			JSONObject msg;
			try {
				msg = JSON.parseObject(line);
			} catch (RuntimeException e) {
				// Not our message. Maelstrom itself rejects such lines, so there is
				// nothing useful to do but note it.
				logger.error("unparseable line: {}", line);
				continue;
			}
			handle(msg);
		}
	}

	private void handle(JSONObject msg) {
		JSONObject body = msg.getJSONObject("body");
		if (body == null) {
			return;
		}
		String type = body.getString("type");
		if (type == null) {
			return;
		}

		if (MaelstromNetwork.MSG_TYPE.equals(type)) {
			// Consensus traffic from a peer, tunnelled through Maelstrom's network.
			//
			// A peer can only address us once it knows our address, and it learns that
			// from its own init, which Maelstrom delivers before the workload starts --
			// but the two inits are not ordered with respect to each other. So a peer
			// that comes up first may emit consensus traffic while we are still
			// initialising. Dropping it is correct: we have no instance to apply it to
			// yet, and a peer that matters will re-propose. Crashing here would turn a
			// startup race into a dead node.
			if (network == null) {
				return;
			}
			deliverPaxosMessage(msg, body);
			return;
		}

		if ("init".equals(type)) {
			handleInit(msg, body);
			return;
		}

		if (body.containsKey("in_reply_to")) {
			// We originate no RPCs, so nothing should reply to us.
			return;
		}

		if ("read".equals(type) || "write".equals(type) || "cas".equals(type)) {
			// Serve off the reader thread: a propose blocks on consensus, and blocking
			// the reader would stall message delivery for this node's peers, including
			// the consensus messages it needs to make progress.
			final String src = msg.getString("src");
			workers.execute(new Runnable() {
				@Override
				public void run() {
					serve(src, body);
				}
			});
		}
	}

	// ---------------------------------------------------------------- init

	private void handleInit(JSONObject msg, JSONObject body) {
		this.nodeId = body.getString("node_id");
		List<String> ids = body.getJSONArray("node_ids").toJavaList(String.class);
		this.nodeIds = ids;
		this.groupCount = Math.max(1, ids.size());

		Map<Integer, String> book = NodeTopology.addressBook(ids);
		this.network = new MaelstromNetwork(book, new MaelstromNetwork.Sink() {
			@Override
			public void sendPaxosMessage(String dest, int groupIdx, byte[] message) {
				send(dest, paxosEnvelope(groupIdx, message));
			}
		}, groupCount);

		// Data root is per node and derived from the node id, so a node killed and
		// restarted by Maelstrom's kill nemesis comes back onto the same physiclog
		// and replays it. Without that, "restart" would mean "forgot everything",
		// which is a client artifact, not a WPaxos bug.
		this.dataRoot = System.getProperty("wpaxos.maelstrom.dataDir", System.getProperty("java.io.tmpdir"))
				+ java.io.File.separator + "wpaxos-maelstrom-" + nodeId;
		new java.io.File(dataRoot).mkdirs();

		try {
			startPaxos();
		} catch (Exception e) {
			// Failing loudly is the honest outcome. If we cannot form a cluster we
			// must not serve reads from local state, because that would look like a
			// cluster that works. Exiting non-zero lets Maelstrom report this node as
			// crashed, with this reason attached, instead of timing out on init.
			logger.error("paxos init failed", e);
			System.err.println("wpaxos-maelstrom: paxos init failed, refusing to serve");
			e.printStackTrace();
			System.exit(1);
		}

		send(msg.getString("src"), okBody(reqId(msg), "init_ok"));
	}

	private void startPaxos() throws Exception {
		List<NodeInfo> members = new ArrayList<NodeInfo>();
		Map<Integer, ArrayList<NodeInfo>> perGroup = new java.util.HashMap<Integer, ArrayList<NodeInfo>>();
		for (String id : nodeIds) {
			members.add(new NodeInfo("127.0.0.1", NodeTopology.portFor(id)));
		}
		for (int g = 0; g < groupCount; g++) {
			// Every group holds every node. Groups are independent Paxos instances
			// that share the network and the disk, not the membership.
			perGroup.put(g, new ArrayList<NodeInfo>(members));
		}

		Options options = new Options();
		options.setLogStoragePath(dataRoot + java.io.File.separator + "log");
		options.setGroupCount(groupCount);
		options.setMyNode(new NodeInfo("127.0.0.1", NodeTopology.portFor(nodeId)));
		options.setNodeInfoList(members);
		options.setNodeInfoMap(perGroup);

		options.setUseMembership(true);
		options.setUseBatchPropose(false);
		options.setNetWork(network);

		// FileIndexDB, not LevelDB. This is forced, not chosen: leveldbjni-all 1.8
		// ships only linux32, linux64 and osx (x86_64) natives, so there is no
		// aarch64 build and LevelDB cannot run on Apple Silicon at all. PHYSIC_FILE
		// is also the documented choice for a small number of groups.
		options.setIndexType(IndexType.PHYSIC_FILE);

		options.setStoreConfig(new com.wuba.wpaxos.store.config.StoreConfig(dataRoot + java.io.File.separator + "store", null));

		machines = new LinKvStateMachine[groupCount];
		for (int g = 0; g < groupCount; g++) {
			GroupSMInfo smInfo = new GroupSMInfo();
			smInfo.setGroupIdx(g);
			// Master election is a WPaxos feature, not part of consensus safety, and
			// leaving it on would add lease expiry and master churn to the histories we
			// are trying to read. Off keeps the experiment on the thing under test.
			smInfo.setUseMaster(false);
			machines[g] = new LinKvStateMachine();
			smInfo.getSmList().add(machines[g]);
			options.getGroupSMInfoList().add(smInfo);
		}

		for (int g = 0; g < groupCount; g++) {
			options.disableMasterElection(g);
		}

		this.paxosNode = Node.runNode(options);
		if (this.paxosNode == null) {
			// Node.runNode returns null when PNode.init fails, and it logs the reason
			// itself. The commonest cause here is a paxos log that failed its own
			// recovery after a kill -- which is a finding about WPaxos, not about this
			// harness, and must not be masked. Say so plainly and refuse to start,
			// rather than dereferencing null and reporting a NullPointerException that
			// would send whoever reads the log looking in the wrong place.
			logger.error("Node.runNode returned null: this node failed to initialise its "
					+ "paxos log. Refusing to serve; see the recovery errors above.");
			throw new IllegalStateException("paxos log recovery failed; refusing to serve");
		}

		// Keep the whole log. The cleaner's delete condition is
		// minChosen + holdCount < checkpointInstanceId + 1, and with no checkpoint the
		// right-hand side is 0, so nothing is ever removed. That is what keeps a
		// full replay from instance 0 possible on every restart, which is what the
		// kill nemesis depends on.
		this.paxosNode.setHoldPaxosLogCount(Long.MAX_VALUE);
	}

	// ---------------------------------------------------------------- serving

	private void serve(String clientNodeId, JSONObject body) {
		String type = body.getString("type");
		long inReplyTo = body.getLongValue("msg_id");
		LinKvStateMachine.Op op;
		try {
			String key = String.valueOf(body.get("key"));
			if ("read".equals(type)) {
				op = LinKvStateMachine.Op.read(key);
			} else if ("write".equals(type)) {
				op = LinKvStateMachine.Op.write(key, body.get("value"));
			} else {
				op = LinKvStateMachine.Op.cas(key, body.get("from"), body.get("to"));
			}
		} catch (RuntimeException e) {
			// A request we cannot even parse definitely did not happen.
			send(clientNodeId, errorBody(inReplyTo, 12, "malformed lin-kv request"));
			return;
		}

		int group = groupOf(op.key);
		LinKvStateMachine.Result result = propose(group, op);
		if (result == null) {
			// Indefinite failure. Consensus did not resolve in time, so the operation
			// may or may not have taken effect; saying "definitely did not happen"
			// would be a lie the checker would rightly punish. Error 13 is indefinite.
			send(clientNodeId, errorBody(inReplyTo, ERR_CRASH, "propose did not complete"));
			return;
		}

		switch (type) {
			case "read":
				if (result.present) {
					send(clientNodeId, readOkBody(inReplyTo, result.value));
				} else {
					// lin-kv defines a read of an absent key as error 20, not a null
					// value. doc/workloads.md is explicit that "unlike lin-kv,
					// nonexistent keys should be returned as null" for other workloads.
					send(clientNodeId,
							errorBody(inReplyTo, ERR_KEY_DOES_NOT_EXIST, "key does not exist"));
				}
				break;
			case "write":
				send(clientNodeId, okBody(inReplyTo, "write_ok"));
				break;
			case "cas":
				if (result.status == LinKvStateMachine.Result.OK) {
					send(clientNodeId, okBody(inReplyTo, "cas_ok"));
				} else if (result.status == LinKvStateMachine.Result.KEY_MISSING) {
					send(clientNodeId,
							errorBody(inReplyTo, ERR_KEY_DOES_NOT_EXIST, "key does not exist"));
				} else {
					// Definite, and legitimately so: the CAS was decided by the state
					// machine at a definite point in the total order, so we know it did
					// not take effect. Contrast with the indefinite error above.
					send(clientNodeId, errorBody(inReplyTo, 22, "precondition failed"));
				}
				break;
			default:
				send(clientNodeId, errorBody(inReplyTo, ERR_TEMPORARILY_UNAVAILABLE, "unsupported"));
				break;
		}
	}

	/**
	 * Proposes an operation and waits for its result.
	 *
	 * @return the state machine's answer, or null if consensus did not confirm this
	 *         node's proposal. A null return must be reported as an <em>indefinite</em>
	 *         error: the value may yet have been chosen.
	 */
	private LinKvStateMachine.Result propose(int group, LinKvStateMachine.Op op) {
		LinKvStateMachine.Result result = new LinKvStateMachine.Result();
		SMCtx ctx = new SMCtx(LinKvStateMachine.SMID, result);
		JavaOriTypeWrapper<Long> instanceId = new JavaOriTypeWrapper<Long>(0L);
		ProposeResult proposeResult;
		try {
			proposeResult = paxosNode.propose(group, op.encode(), instanceId, ctx, PROPOSE_TIMEOUT_MS);
		} catch (RuntimeException e) {
			logger.error("propose threw", e);
			return null;
		}
		if (proposeResult == null
				|| proposeResult.getResult() != PaxosTryCommitRet.PaxosTryCommitRet_OK.getRet()) {
			return null;
		}
		return result;
	}

	/**
	 * The key-to-group mapping: {@code floorMod(key.hashCode(), groupCount)}.
	 *
	 * <p>{@code floorMod} rather than {@code %} because {@code hashCode} may be
	 * {@link Integer#MIN_VALUE}, whose remainder is negative, and a negative group
	 * index would be rejected by WPaxos.
	 */
	int groupOf(String key) {
		return Math.floorMod(key.hashCode(), groupCount);
	}

	// ---------------------------------------------------------------- transport

	private void deliverPaxosMessage(JSONObject msg, JSONObject body) {
		String data = body.getString("data");
		if (data == null) {
			return;
		}
		byte[] raw;
		try {
			raw = java.util.Base64.getDecoder().decode(data);
		} catch (IllegalArgumentException e) {
			logger.error("corrupt tunnelled message from {}", msg.getString("src"));
			return;
		}
		network.deliver(raw);
	}

	private JSONObject paxosEnvelope(int groupIdx, byte[] message) {
		JSONObject body = new JSONObject();
		body.put("type", MaelstromNetwork.MSG_TYPE);
		body.put("group", groupIdx);
		body.put("data", java.util.Base64.getEncoder().encodeToString(message));
		return body;
	}

	// ---------------------------------------------------------------- replies

	private void send(String dest, JSONObject body) {
		JSONObject msg = new JSONObject();
		msg.put("src", nodeId);
		msg.put("dest", dest);
		msg.put("body", body);
		String line = msg.toJSONString();
		synchronized (writeLock) {
			out.println(line);
			out.flush();
		}
	}

	private JSONObject okBody(long inReplyTo, String type) {
		JSONObject body = new JSONObject();
		body.put("type", type);
		body.put("in_reply_to", inReplyTo);
		return body;
	}

	private JSONObject readOkBody(long inReplyTo, Object value) {
		JSONObject body = new JSONObject();
		body.put("type", "read_ok");
		body.put("in_reply_to", inReplyTo);
		body.put("value", value);
		return body;
	}

	private JSONObject errorBody(long inReplyTo, int code, String text) {
		JSONObject body = new JSONObject();
		body.put("type", "error");
		body.put("in_reply_to", inReplyTo);
		body.put("code", code);
		body.put("text", text);
		return body;
	}

	private long reqId(JSONObject req) {
		JSONObject body = req.getJSONObject("body");
		return body == null ? -1L : body.getLongValue("msg_id");
	}
}
