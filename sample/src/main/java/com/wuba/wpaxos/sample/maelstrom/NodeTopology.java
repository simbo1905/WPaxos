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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps Maelstrom node ids onto the {@code (ip, port)} pairs WPaxos needs.
 *
 * <p>Maelstrom runs every node as a separate process on the same host and names
 * them {@code n1}, {@code n2}, ... WPaxos identifies a member by ip:port, and
 * {@link com.wuba.wpaxos.comm.NodeInfo} hashes that pair into its numeric node id.
 * So we hand out one loopback port per Maelstrom node id, which yields distinct
 * WPaxos node ids.
 *
 * <p>Those ports are <em>never bound</em>. {@link MaelstromNetwork} carries
 * consensus traffic over stdio instead, so the ports exist purely as identity. That
 * is deliberate: it means a killed node restarting on the same port can never lose
 * its address, and two concurrent runs on one host cannot collide.
 *
 * <p>The mapping is derived purely from the node id, so it is stable across a
 * kill/restart. A restarted node must compute the same port for itself and for
 * every peer, or the restarted node would address a different cluster.
 */
public final class NodeTopology {

	/** First port handed out. Low enough to stay clear of WPaxos sample defaults. */
	public static final int BASE_PORT = 30000;

	private NodeTopology() {
	}

	/**
	 * Extracts the ordinal from a Maelstrom node id of the form {@code n<k>}.
	 *
	 * @throws IllegalArgumentException if the id is not of that shape, because a node
	 *                                  id we cannot parse means we cannot address
	 *                                  the node at all, and silently guessing would
	 *                                  produce a cluster that disagrees with itself.
	 */
	public static int ordinalOf(String nodeId) {
		if (nodeId == null || nodeId.length() < 2 || nodeId.charAt(0) != 'n') {
			throw new IllegalArgumentException("Unrecognised Maelstrom node id: " + nodeId);
		}
		try {
			return Integer.parseInt(nodeId.substring(1));
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Unrecognised Maelstrom node id: " + nodeId, e);
		}
	}

	/** The port this node will use, given its own Maelstrom node id. */
	public static int portFor(String nodeId) {
		return BASE_PORT + ordinalOf(nodeId);
	}

	/**
	 * Builds the full address book from the {@code node_ids} list of the init message.
	 *
	 * <p>Every node receives an identical {@code node_ids} list, so every node builds
	 * an identical book. That agreement is what lets a restarted node rejoin: it
	 * re-derives the same addresses for itself and its peers from the same input.
	 *
	 * @return port to node id, in the order Maelstrom listed them.
	 */
	public static Map<Integer, String> addressBook(List<String> nodeIds) {
		Map<Integer, String> book = new LinkedHashMap<Integer, String>();
		for (String id : nodeIds) {
			book.put(portFor(id), id);
		}
		return book;
	}

	/**
	 * The node ids of every member except the one given, preserving Maelstrom's order.
	 *
	 * <p>Maelstrom's order is stable across nodes and across restarts, so deriving
	 * membership from it -- rather than from each node's local view -- keeps every
	 * node's notion of the cluster identical.
	 */
	public static List<String> peersOf(List<String> nodeIds, String self) {
		List<String> peers = new ArrayList<String>();
		for (String id : nodeIds) {
			if (!id.equals(self)) {
				peers.add(id);
			}
		}
		return peers;
	}
}
