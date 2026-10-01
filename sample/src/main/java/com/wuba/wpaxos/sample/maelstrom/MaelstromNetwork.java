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

import java.util.Map;
import java.util.Set;

import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.communicate.NetWork;

/**
 * A {@link NetWork} that carries every WPaxos message over the Maelstrom stdio
 * JSON-lines network instead of real TCP/UDP sockets.
 *
 * <p>This is the whole point of the harness. Maelstrom's nemesis (partition, kill,
 * pause) and its latency/loss injection only govern traffic that traverses
 * Maelstrom's simulated network, which is exactly the traffic a node writes to
 * STDOUT as JSON. WPaxos's stock {@code DFNetWorker} opens real localhost TCP and
 * UDP sockets, which Maelstrom cannot see, partition, pause or drop. Running the
 * stock network would mean the nemesis only ever perturbs client traffic, and the
 * consensus implementation would never be tested at all.
 *
 * <p>Tunnelling consensus through stdio makes Maelstrom a black box for WPaxos: a
 * Maelstrom partition now severs a quorum, a kill now stops an acceptor from
 * voting, and a pause now freezes a learner. Those are precisely the failure modes
 * where a consensus safety bug lives.
 *
 * <p>The TCP/UDP distinction is deliberately collapsed. Maelstrom's network already
 * supplies its own delivery semantics (reordering, loss, partitions, bounded
 * backlogs for down nodes), and the stock split exists only to mirror the kernel's
 * behaviour on a real LAN. Keeping it would buy nothing here and would cost the
 * ability to reason about delivery.
 *
 * <p>Node identity: Maelstrom node ids look like {@code n1}, {@code n2}, ... The
 * harness assigns each one a distinct loopback port (see {@link NodeTopology}) so
 * that {@link NodeInfo} -- which derives its numeric id from ip:port -- assigns
 * every node a distinct id. Those ports are never bound; they exist only as
 * identity. That is why two concurrent Maelstrom runs cannot collide on a port.
 */
public class MaelstromNetwork extends NetWork {

	/**
	 * Message type used for tunnelled WPaxos consensus traffic. Deliberately not one
	 * of the workload RPC types, so Maelstrom's workload client never sees it.
	 */
	public static final String MSG_TYPE = "wpaxos_paxos_msg";

	/**
	 * Where an encoded consensus message goes. Split out so the network does not need
	 * to know how the protocol loop writes to STDOUT.
	 */
	public interface Sink {
		void sendPaxosMessage(String destNodeId, int groupIdx, byte[] message);
	}

	private final Map<Integer, String> portToNodeId;
	private final Sink sink;
	private final int maxGroupIdx;

	/**
	 * @param portToNodeId WPaxos listen port to Maelstrom node id, used to address the
	 *                      peer of an outgoing message.
	 * @param sink         the sink that writes the encoded message to STDOUT.
	 * @param maxGroupIdx  number of paxos groups this node runs.
	 */
	public MaelstromNetwork(Map<Integer, String> portToNodeId, Sink sink, int maxGroupIdx) {
		this.portToNodeId = portToNodeId;
		this.sink = sink;
		this.maxGroupIdx = maxGroupIdx;
	}

	@Override
	public void runNetWork() {
		// Nothing to start: there is no socket to bind. Maelstrom owns the transport,
		// and it is already running our STDOUT reader by the time it sends us init.
	}

	@Override
	public void stopNetWork() {
		// Nothing to stop.
	}

	@Override
	public int sendMessageTCP(int groupIdx, String ip, int port, byte[] message) {
		return send(groupIdx, port, message);
	}

	@Override
	public int sendMessageUDP(int groupIdx, String ip, int port, byte[] message) {
		return send(groupIdx, port, message);
	}

	private int send(int groupIdx, int port, byte[] message) {
		if (message == null || message.length == 0) {
			return -1;
		}
		if (groupIdx < 0 || groupIdx >= maxGroupIdx) {
			return -1;
		}
		String dest = portToNodeId.get(port);
		if (dest == null) {
			// The peer is not a member of this cluster. WPaxos only ever addresses
			// nodes from its membership list, so this means the membership and the
			// topology disagree, which is a harness bug worth surfacing loudly.
			return -1;
		}
		sink.sendPaxosMessage(dest, groupIdx, message);
		return 0;
	}

	@Override
	public void setCheckNode(int group, Set<NodeInfo> nodeInfos) {
		// No connection state to keep alive: Maelstrom's network has no notion of a
		// half-open socket for us to prune.
	}

	/**
	 * Feeds a consensus message that arrived on STDIN into WPaxos.
	 *
	 * <p>Called by the protocol loop, not by WPaxos.
	 */
	public void deliver(byte[] message) {
		onReceiveMessage(new com.wuba.wpaxos.communicate.ReceiveMessage(message, message.length));
	}
}
