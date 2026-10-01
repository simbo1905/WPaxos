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

import com.wuba.wpaxos.Acceptor;
import com.wuba.wpaxos.Instance;
import com.wuba.wpaxos.Proposer;
import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.comm.enums.PaxosMsgType;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.proto.PaxosMsg;

/**
 * Proof of Defect #1 in ticket 483 (Acceptor persist failure path):
 *
 * In Acceptor.java onAccept / updateAcceptorState4Accept:
 * When acceptorState.persist fails (e.g. disk write failure), updateAcceptorState4Accept
 * returns void without setting rejectByPromiseID on replyPaxosMsg.
 * onAccept then sends replyPaxosMsg with rejectByPromiseID == 0.
 *
 * When the proposer receives onAcceptReply, seeing rejectByPromiseID == 0,
 * it treats the vote as an accept and adds it to its msgCounter (addPromiseOrAccept).
 * This counts an unpersisted accept towards a quorum, violating Paxos agreement
 * and durability invariants.
 */
public class AcceptorPersistFailureSafetyTest {

	@Test
	public void testAcceptorMustRejectWhenPersistFailsOnAccept() throws Exception {
		MemoryLogStorage logStorage = new MemoryLogStorage();
		TestSupport.TestMsgTransport transport = new TestSupport.TestMsgTransport();
		Config config = TestSupport.createConfig(logStorage, true);
		Options options = TestSupport.createOptions();
		Instance instance = TestSupport.createInstance(config, logStorage, transport, options);

		Acceptor acceptor = new Acceptor(config, transport, instance, logStorage);
		acceptor.init();

		// Simulate disk persistence failure
		logStorage.setFailPut(true);

		// Send an Accept message to this acceptor from remote proposer node 20002
		long remoteProposerNodeID = 20002L;
		PaxosMsg acceptMsg = new PaxosMsg();
		acceptMsg.setInstanceID(0);
		acceptMsg.setNodeID(remoteProposerNodeID);
		acceptMsg.setProposalID(1);
		acceptMsg.setMsgType(PaxosMsgType.paxosAccept.getValue());
		acceptMsg.setValue("safety-critical-value".getBytes());

		transport.clear();
		acceptor.onAccept(acceptMsg);

		// Verify what was sent
		PaxosMsg reply = transport.getLastSentMessage();
		Assert.assertNotNull("Acceptor must have sent a reply", reply);
		Assert.assertEquals(PaxosMsgType.paxosAcceptReply.getValue(), reply.getMsgType());

		// SAFETY INVARIANT: If persistence failed, the acceptor MUST NOT send a success reply
		// (rejectByPromiseID must be > 0 so that proposer knows it was rejected).
		// Currently in WPaxos, updateAcceptorState4Accept swallows the failure and rejectByPromiseID remains 0,
		// causing the proposer to count this unpersisted vote as an accept!
		Assert.assertTrue(
				"SAFETY DEFECT #1 CONFIRMED: When persistence fails on accept, reply must reject proposal "
						+ "(rejectByPromiseID > 0), but rejectByPromiseID was " + reply.getRejectByPromiseID()
						+ ", which causes proposer to count this unpersisted vote as an accept!",
				reply.getRejectByPromiseID() > 0
		);
	}
}
