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
package com.wuba.wpaxos.safety;

import org.junit.Assert;
import org.junit.Test;

import com.wuba.wpaxos.Instance;
import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.comm.enums.PaxosMsgType;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.proto.PaxosMsg;

/**
 * Proof of Premature Learning Without Quorum in Instance.java (lines 484-494):
 *
 * In Instance.receiveMsgForAcceptor:
 * <pre>
 *   if (paxosMsg.getInstanceID() == this.acceptor.getInstanceID() + 1) {
 *       // skip success message
 *       PaxosMsg newPaxosMsg = new PaxosMsg();
 *       newPaxosMsg.setInstanceID(this.acceptor.getInstanceID());
 *       newPaxosMsg.setMsgType(PaxosMsgType.paxosLearnerProposerSendSuccess.getValue());
 *       newPaxosMsg.setNodeID(paxosMsg.getNodeID());
 *       newPaxosMsg.setProposalID(paxosMsg.getProposalID());
 *       newPaxosMsg.setProposalNodeID(paxosMsg.getProposalNodeID());
 *       
 *       receiveMsgForLearner(newPaxosMsg);
 *   }
 * </pre>
 *
 * When a node receives a message for instance N + 1 from proposer P, it blindly assumes
 * that instance N succeeded if proposalID matches.
 * If the proposer sent an accept for instance N to ONLY this single node (no quorum was reached),
 * and then moved on or sent a message for instance N + 1, this node immediately marks
 * instance N as learned and commits it to its state machine, even though the value
 * was NEVER accepted by a quorum!
 *
 * If another proposer gets a different value chosen for instance N on the remaining nodes,
 * replicas permanently diverge and violate Paxos consensus safety.
 */
public class PrematureLearningWithoutQuorumSafetyTest {

	@Test
	public void testProposalWithoutQuorumMustNotBePrematurelyLearned() throws Exception {
		MemoryLogStorage logStorage = new MemoryLogStorage();
		TestSupport.TestMsgTransport transport = new TestSupport.TestMsgTransport();
		Config config = TestSupport.createConfig(logStorage, false);
		Options options = TestSupport.createOptions();
		Instance instance = TestSupport.createInstance(config, logStorage, transport, options);

		TestSupport.TestStateMachine sm = new TestSupport.TestStateMachine();
		instance.addStateMachine(sm);
		instance.init();

		long proposerNodeId = 20002L;
		long proposalId = 10L;

		// 1. Acceptor receives an Accept message for instance 0 from proposerNodeId
		PaxosMsg acceptMsg = new PaxosMsg();
		acceptMsg.setInstanceID(0L);
		acceptMsg.setNodeID(proposerNodeId);
		acceptMsg.setProposalID(proposalId);
		acceptMsg.setMsgType(PaxosMsgType.paxosAccept.getValue());
		acceptMsg.setValue(TestSupport.packValue(TestSupport.TestStateMachine.SMID, "abandoned-proposal-no-quorum".getBytes()));

		instance.receiveMsgForAcceptor(acceptMsg, false);

		// Assert that instance 0 is NOT executed yet (only accepted by 1 node, no quorum)
		Assert.assertFalse("Instance 0 must not be executed yet", sm.isExecuted(0L));

		// 2. Now a message for instance 1 arrives with the same proposalId from proposerNodeId.
		// (e.g. proposer skipped ahead, client retry, or pipeline message)
		PaxosMsg nextInstanceMsg = new PaxosMsg();
		nextInstanceMsg.setInstanceID(1L); // instanceID == acceptor.getInstanceID() + 1
		nextInstanceMsg.setNodeID(proposerNodeId);
		nextInstanceMsg.setProposalID(proposalId);
		nextInstanceMsg.setProposalNodeID(proposerNodeId);
		nextInstanceMsg.setMsgType(PaxosMsgType.paxosPrepare.getValue());

		// Process next instance message
		instance.receiveMsgForAcceptor(nextInstanceMsg, false);

		// SAFETY INVARIANT:
		// A proposal that never reached quorum must NEVER be executed on the state machine!
		// Because of the 'skip success message' shortcut in Instance.java (lines 484-494),
		// the node falsely assumes instance 0 succeeded and executes the unchosen value!
		Assert.assertFalse(
				"SAFETY DEFECT CONFIRMED: Proposal without quorum was prematurely learned and executed on StateMachine "
						+ "due to Instance.java skip success message logic!",
				sm.isExecuted(0L)
		);
	}
}
