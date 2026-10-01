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
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.proto.AcceptorStateData;
import com.wuba.wpaxos.store.PaxosLog;

/**
 * Proof of the unchosen accept replay hypothesis in ticket 483:
 *
 * In Paxos, an individual acceptor's acceptedValue (from Phase 2 onAccept) is NOT necessarily
 * chosen by a quorum. A proposer may abandon the proposal, fail, or another proposer with a higher
 * proposal number may get a different value chosen.
 *
 * In Instance.java playLog:
 * <pre>
 *   for (long instanceID = start; instanceID &lt; endInstanceID; instanceID++) {
 *       AcceptorStateData state = new AcceptorStateData();
 *       int ret = this.paxosLog.readState(this.config.getMyGroupIdx(), instanceID, state);
 *       boolean excuteRet = this.smFac.execute(this.config.getMyGroupIdx(), instanceID, state.getAcceptedValue(), null);
 *   }
 * </pre>
 *
 * playLog reads AcceptorStateData from paxosLog and directly executes state.getAcceptedValue()
 * on the StateMachine without checking if the instance was ever chosen by a quorum.
 * If an unchosen accept exists in the log, playLog blindly replays it to the state machine,
 * causing state machine divergence and linearizability violations.
 */
public class PlayLogUnchosenAcceptSafetyTest {

	@Test
	public void testPlayLogMustNotExecuteUnchosenAcceptedValue() throws Exception {
		MemoryLogStorage logStorage = new MemoryLogStorage();
		TestSupport.TestMsgTransport transport = new TestSupport.TestMsgTransport();
		Config config = TestSupport.createConfig(logStorage, true);
		Options options = TestSupport.createOptions();
		Instance instance = TestSupport.createInstance(config, logStorage, transport, options);

		TestSupport.TestStateMachine sm = new TestSupport.TestStateMachine();
		instance.addStateMachine(sm);

		// Record an UNCHOSEN accept in paxosLog for instance 0.
		// (e.g. Node accepted proposal 1 with "unchosen-value", but it never reached quorum).
		AcceptorStateData unchosenState = new AcceptorStateData();
		unchosenState.setInstanceID(0L);
		unchosenState.setPromiseID(1L);
		unchosenState.setPromiseNodeID(1L);
		unchosenState.setAcceptedID(1L);
		unchosenState.setAcceptedNodeID(1L);
		unchosenState.setAcceptedValue(TestSupport.packValue(TestSupport.TestStateMachine.SMID, "unchosen-divergent-value".getBytes()));

		PaxosLog paxosLog = new PaxosLog(logStorage);
		paxosLog.writeState(new WriteOptions(), config.getMyGroupIdx(), 0L, unchosenState);

		// Replay log for instance 0
		int ret = instance.playLog(0L, 1L);
		Assert.assertEquals("playLog return code", 0, ret);

		// SAFETY INVARIANT: A state machine must ONLY execute values that were chosen by a quorum.
		// Replaying an unchosen accept on restart or log recovery corrupts the state machine state.
		// Currently in WPaxos, playLog blindly passes state.getAcceptedValue() to smFac.execute()!
		Assert.assertFalse(
				"SAFETY DEFECT CONFIRMED: playLog executed an unchosen accept on the state machine! "
						+ "Executed value was: " + new String(sm.getExecutedValue(0L)),
				sm.isExecuted(0L)
		);
	}
}
