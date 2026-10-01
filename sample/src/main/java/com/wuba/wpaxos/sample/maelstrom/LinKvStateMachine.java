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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.storemachine.StateMachine;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

/**
 * The lin-kv state machine, backed entirely by WPaxos consensus.
 *
 * <h2>Why the map is only a cache of the log</h2>
 *
 * This state machine deliberately keeps its key/value map <em>only in memory</em> and
 * treats the WPaxos paxos log as the sole source of truth. There is no second store.
 * That is the entire durability story, and it is WPaxos's own story:
 *
 * <ul>
 *   <li>{@code Options.writeSync} defaults to true, so an acceptor's
 *       {@code AcceptorState.persist} fsyncs each accepted value into physiclog
 *       before the accept reply is sent. A value that a quorum has accepted is
 *       therefore on disk.</li>
 *   <li>On restart {@code Instance.init} loads the acceptor state (max instance id,
 *       promise ballot, last accepted value) from the log and then calls
 *       {@code playLog}, which re-executes every chosen value from instance 0 up to
 *       the acceptor's instance id through {@code SMFac.execute}. That replay is what
 *       rebuilds this map.</li>
 *   <li>The index is not a second source of truth either. {@code FileIndexDB} is a
 *       derived index; {@code DefaultDataBase.recover} walks physiclog and rebuilds
 *       any index entries the crash left behind. So the index losing its tail costs
 *       time on restart, not state.</li>
 * </ul>
 *
 * <p>The consequence for this harness is the thing that distinguishes a real WPaxos
 * safety bug from a client artifact: <b>this state machine cannot "lose state on
 * restart"</b>, because it never had any state to lose. If a node is killed and comes
 * back having applied fewer instances than its peers, that is WPaxos failing to
 * replay its own log -- a consensus durability bug, reported as such -- and not a
 * client that forgot to persist a cache. There is no second store for a bug to hide
 * in, and no way for the harness to accidentally paper over a violation.
 *
 * <h2>Why replay is safe</h2>
 *
 * {@code execute} is a deterministic function of (map, op). Replay rebuilds the map
 * from empty by re-applying the same chosen values in the same instance order, so it
 * reaches the same state the live node had. Reads and compare-and-sets are replayed
 * too, and are equally deterministic: a CAS re-decides identically because the map it
 * reads is the same one the original execution read. Results are only recorded when a
 * client is waiting (a non-null {@link SMCtx} context), so replay produces no
 * spurious replies.
 *
 * <p>The log is never truncated, which is what keeps full replay possible.
 * {@code getCheckpointInstanceID} returns -1 (no checkpointing), so the cleaner's
 * precondition {@code minChosen + holdCount < checkpointInstanceId + 1} is
 * {@code 0 + holdCount < 0}, which is never true. Nothing is ever deleted, so
 * instance 0 stays readable and every restart can replay the whole history.
 *
 * <h2>Why every operation goes through consensus</h2>
 *
 * Reads and CAS are proposed, not served locally. This is a correctness requirement,
 * not a stylistic one:
 *
 * <ul>
 *   <li>A local read would return whatever this node has applied, and a node that has
 *       not yet learned the latest chosen value would return a stale one. Maelstrom's
 *       linearizability checker would rightly report that as a violation -- and it
 *       would be <em>our</em> fault, not WPaxos's. That is precisely the false
 *       positive this harness exists to avoid.</li>
 *   <li>Routing the read through the log puts it in the same total order as the
 *       writes, so the value it observes is the value at a definite point in that
 *       order. That is linearizability, obtained by construction.</li>
 *   <li>CAS must be decided at a definite point in the total order anyway. Deciding
 *       it inside the state machine is what makes a "precondition failed" reply a
 *       <em>definite</em> error (code 22) rather than an indefinite one.</li>
 * </ul>
 */
public class LinKvStateMachine implements StateMachine {

	/**
	 * State machine id. Must not collide with {@code Def.SYSTEM_V_SMID},
	 * {@code Def.MASTER_V_SMID} or {@code Def.BATCH_PROPOSE_SMID}, and must be stable
	 * across restarts because the id is written into every log record.
	 */
	public static final int SMID = 3;

	/** Operation kinds, as carried in the log. */
	public static final int OP_READ = 0;
	public static final int OP_WRITE = 1;
	public static final int OP_CAS = 2;

	/**
	 * The key/value map. A cache of the chosen prefix of the paxos log, and nothing
	 * more: it is rebuilt from scratch by replay on every start, so it is never
	 * persisted and never consulted for anything but the current applied prefix.
	 */
	private final Map<String, Object> map = new ConcurrentHashMap<String, Object>();

	/**
	 * Answer handed back to a waiting client through {@link SMCtx}.
	 *
	 * <p>Only ever populated on the node whose proposal was chosen for that instance;
	 * WPaxos matches the committed value against the proposed one and only then
	 * attaches the context. A node that merely executes someone else's value has no
	 * context and records nothing.
	 */
	public static class Result {
		/** One of {@link #OK}, {@link #KEY_MISSING}, {@link #PRECONDITION_FAILED}. */
		public int status = OK;
		/** Value observed by a read, as the raw JSON value; null when absent. */
		public Object value;
		/** True when a read found the key present. */
		public boolean present;

		public static final int OK = 0;
		public static final int KEY_MISSING = 1;
		public static final int PRECONDITION_FAILED = 2;
	}

	@Override
	public int getSMID() {
		return SMID;
	}

	@Override
	public boolean execute(int groupIdx, long instanceID, byte[] paxosValue, SMCtx smCtx) {
		Op op;
		try {
			op = Op.decode(paxosValue);
		} catch (RuntimeException e) {
			// A record we cannot parse will not become parseable by retrying, and
			// retrying forever would stall the instance. Report success so the log
			// advances; there is nothing this node can do about the record either way.
			return true;
		}

		Result result = resultOf(smCtx);
		String key = op.key;

		switch (op.kind) {
			case OP_WRITE:
				map.put(key, op.value);
				markPresent(result);
				break;

			case OP_READ:
				if (map.containsKey(key)) {
					if (result != null) {
						result.present = true;
						result.value = map.get(key);
					}
				} else {
					if (result != null) {
						result.present = false;
						result.value = null;
						result.status = Result.KEY_MISSING;
					}
				}
				break;

			case OP_CAS: {
				boolean present = map.containsKey(key);
				if (!present) {
					// Definite: decided here, at a definite point in the total order.
					if (result != null) {
						result.status = Result.KEY_MISSING;
					}
					break;
				}
				if (!jsonEquals(map.get(key), op.from)) {
					if (result != null) {
						result.status = Result.PRECONDITION_FAILED;
					}
					break;
				}
				map.put(key, op.value);
				markPresent(result);
				break;
			}

			default:
				break;
		}
		return true;
	}

	private static void markPresent(Result result) {
		if (result != null) {
			result.status = Result.OK;
			result.present = true;
		}
	}

	/**
	 * Extracts the caller's result holder, or null when nobody is waiting.
	 *
	 * <p>Null on two paths, both benign: replay after a restart, and execution on a
	 * node that did not propose the chosen value.
	 */
	private static Result resultOf(SMCtx smCtx) {
		if (smCtx == null) {
			return null;
		}
		Object ctx = smCtx.getpCtx();
		return ctx instanceof Result ? (Result) ctx : null;
	}

	/**
	 * Structural JSON equality, used by CAS.
	 *
	 * <p>Compares the serialised form rather than {@code equals} so that a value
	 * which arrived as {@code Integer 3} matches one that arrived as {@code Long 3}.
	 * CAS is decided by the state machine, so an inconsistent notion of equality here
	 * would show up as a spurious precondition failure.
	 */
	static boolean jsonEquals(Object a, Object b) {
		if (a == null || b == null) {
			return a == b;
		}
		return JSON.toJSONString(a).equals(JSON.toJSONString(b));
	}

	@Override
	public boolean executeForCheckpoint(int groupIdx, long instanceID, byte[] paxosValue) {
		// No checkpointing: this harness always replays the full log from instance 0,
		// so there is never a checkpoint for the replayer to regenerate.
		return true;
	}

	@Override
	public long getCheckpointInstanceID(int groupIdx) {
		// -1 means "this state machine has no checkpoint". smFac then yields a
		// checkpoint instance id of -1, and Instance.init replays from instance 0.
		return -1;
	}

	@Override
	public int lockCheckpointState() {
		return 0;
	}

	@Override
	public int getCheckpointState(int groupIdx, JavaOriTypeWrapper<String> dirPath, List<String> fileList) {
		return 0;
	}

	@Override
	public void unLockCheckpointState() {
	}

	@Override
	public int loadCheckpointState(int groupIdx, String checkpointTmpFileDirPath, List<String> fileList,
			long checkpointInstanceID) {
		return 0;
	}

	@Override
	public byte[] beforePropose(int groupIdx, byte[] sValue) {
		// Values are proposed verbatim. In particular a CAS must not be rewritten
		// client-side: its expected value is part of the operation's meaning.
		return null;
	}

	@Override
	public boolean needCallBeforePropose() {
		return false;
	}

	@Override
	public void fixCheckpointByMinChosenInstanceId(long minChosenInstanceID) {
	}

	/** Test/diagnostic accessor. Not used by the protocol path. */
	public Map<String, Object> snapshot() {
		return map;
	}

	/**
	 * A single lin-kv operation as it is written into the paxos log.
	 *
	 * <p>Key and value are carried as raw JSON so any JSON type Maelstrom can send is
	 * representable, and so a value read back out is byte-identical to the one written.
	 */
	static final class Op {
		int kind;
		String key;
		Object value;
		Object from;

		static Op read(String key) {
			Op op = new Op();
			op.kind = OP_READ;
			op.key = key;
			return op;
		}

		static Op write(String key, Object value) {
			Op op = new Op();
			op.kind = OP_WRITE;
			op.key = key;
			op.value = value;
			return op;
		}

		static Op cas(String key, Object from, Object to) {
			Op op = new Op();
			op.kind = OP_CAS;
			op.key = key;
			op.from = from;
			op.value = to;
			return op;
		}

		byte[] encode() {
			StringBuilder sb = new StringBuilder();
			sb.append("{\"k\":").append(kind);
			sb.append(",\"key\":").append(JSON.toJSONString(key));
			if (kind == OP_WRITE || kind == OP_CAS) {
				sb.append(",\"value\":").append(JSON.toJSONString(value));
			}
			if (kind == OP_CAS) {
				sb.append(",\"from\":").append(JSON.toJSONString(from));
			}
			sb.append('}');
			return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
		}

		static Op decode(byte[] bytes) {
			@SuppressWarnings("unchecked")
			Map<String, Object> m = (Map<String, Object>) JSON.parseObject(new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
			if (m == null) {
				throw new JSONException("empty op record");
			}
			Op op = new Op();
			Object k = m.get("k");
			op.kind = k instanceof Number ? ((Number) k).intValue() : -1;
			Object key = m.get("key");
			op.key = key == null ? null : String.valueOf(key);
			op.value = m.get("value");
			op.from = m.get("from");
			return op;
		}
	}
}
