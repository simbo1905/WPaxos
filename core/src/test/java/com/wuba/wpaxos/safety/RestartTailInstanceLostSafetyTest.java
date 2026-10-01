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
 * Proof of Ticket 480 replica divergence root cause:
 *
 * In ticket 480, after a node restart, replica n0 served stale read of 2 after n1 had written 3.
 *
 * Root cause in Instance.java init():
 * <pre>
 *   long nowInstanceID = cpInstanceID; // e.g. 0
 *   if (nowInstanceID &lt; this.acceptor.getInstanceID()) {
 *       ret = playLog(nowInstanceID, this.acceptor.getInstanceID());
 *       nowInstanceID = this.acceptor.getInstanceID();
 *   } else {
 *       this.acceptor.setInstanceID(nowInstanceID);
 *   }
 * </pre>
 *
 * 1) Acceptor.init() sets acceptor.instanceID to paxosLog.getMaxInstanceIDFromLog() (the 0-indexed highest logged instance).
 *    If instance 0 was committed in the log, maxInstanceID is 0.
 *    Then nowInstanceID (0) &lt; acceptor.instanceID (0) is FALSE!
 *    playLog is NEVER invoked for instance 0!
 * 2) Even if multiple instances were committed (e.g. instances 0 and 1, maxInstanceID = 1):
 *    playLog(0, 1) loops `instanceID &lt; endInstanceID`, which executes ONLY instance 0,
 *    and EXCLUDES instance 1 (the tail instance)!
 * 3) In all cases, the highest chosen instance in the log is NEVER replayed to the StateMachine on restart,
 *    and the node's instance counter resets to that instance, causing stale reads (ticket 480)
 *    and allowing committed instances to be overwritten!
 */
public class RestartTailInstanceLostSafetyTest {

	@Test
	public void testRestartMustReplayTailChosenInstanceToStateMachine() throws Exception {
		MemoryLogStorage logStorage = new MemoryLogStorage();
		Config config = TestSupport.createConfig(logStorage, true);

		// Record chosen instance 0 in the log (e.g. key=x, value=3 written before crash)
		byte[] chosenValue = TestSupport.packValue(TestSupport.TestStateMachine.SMID, "chosen-write-3".getBytes());
		AcceptorStateData state = new AcceptorStateData();
		state.setInstanceID(0L);
		state.setPromiseID(1L);
		state.setPromiseNodeID(config.getMyNodeID());
		state.setAcceptedID(1L);
		state.setAcceptedNodeID(config.getMyNodeID());
		state.setAcceptedValue(chosenValue);

		PaxosLog paxosLog = new PaxosLog(logStorage);
		paxosLog.writeState(new WriteOptions(true), config.getMyGroupIdx(), 0L, state);

		// Verify max instance in log is 0
		com.wuba.wpaxos.utils.JavaOriTypeWrapper<Long> maxInstanceWrapper = new com.wuba.wpaxos.utils.JavaOriTypeWrapper<>(0L);
		paxosLog.getMaxInstanceIDFromLog(config.getMyGroupIdx(), maxInstanceWrapper);
		Assert.assertEquals(0L, (long) maxInstanceWrapper.getValue());

		// Now simulate node restart by initializing a new Instance from the persisted log
		TestSupport.TestMsgTransport transport = new TestSupport.TestMsgTransport();
		Options options = TestSupport.createOptions();
		Instance restartedInstance = TestSupport.createInstance(config, logStorage, transport, options);

		TestSupport.TestStateMachine sm = new TestSupport.TestStateMachine();
		restartedInstance.addStateMachine(sm);

		int ret = restartedInstance.init();
		Assert.assertEquals("Instance.init should succeed", 0, ret);

		// SAFETY INVARIANT (Ticket 480):
		// On restart, the node must replay all chosen instances from the log into its state machine.
		// If instance 0 is not replayed, the state machine remains empty/stale, serving stale data (reading 2 instead of 3).
		// Furthermore, nowInstanceID should advance to 1 (the next uncommitted instance), not stay at 0.
		Assert.assertTrue(
				"SAFETY DEFECT (TICKET 480) CONFIRMED: Tail chosen instance was NOT replayed on restart! "
						+ "StateMachine is empty/stale because playLog excluded or skipped the tail instance. "
						+ "nowInstanceID is " + restartedInstance.getNowInstanceID() + " (expected 1)",
				sm.isExecuted(0L)
		);
	}
}
