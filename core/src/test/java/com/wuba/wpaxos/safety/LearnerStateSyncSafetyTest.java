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

import com.wuba.wpaxos.base.BallotNumber;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.helper.LearnerState;

/**
 * Proof of Defect #3 in ticket 483 (LearnerState.learnValue hardcodes writeOptions.setSync(false)):
 *
 * In LearnerState.java:
 * <pre>
 *   WriteOptions writeOptions = new WriteOptions();
 *   writeOptions.setSync(false);
 *   int ret = this.paxosLog.writeState(writeOptions, this.config.getMyGroupIdx(), instanceID, state);
 * </pre>
 *
 * Even when the cluster is configured with Options.writeSync = true, the learn path
 * forcibly overrides writeOptions.setSync(false). As a result, when a replica catches up
 * or learns chosen values from peers, those values are written to page cache without fsync.
 * If the node crashes or restarts shortly thereafter, learned consensus entries can be lost,
 * violating durability and Paxos consensus history continuity.
 */
public class LearnerStateSyncSafetyTest {

	@Test
	public void testLearnerStateMustHonorConfiguredSyncOption() throws Exception {
		MemoryLogStorage logStorage = new MemoryLogStorage();
		// Configure with writeSync = true (the production default for durability)
		Config config = TestSupport.createConfig(logStorage, true);

		LearnerState learnerState = new LearnerState(config, logStorage);

		long instanceID = 1L;
		BallotNumber ballot = new BallotNumber(1, 1);
		byte[] value = "learned-value-requiring-durable-sync".getBytes();

		int ret = learnerState.learnValue(instanceID, ballot, value, 0);
		Assert.assertEquals(0, ret);

		WriteOptions usedOptions = logStorage.getLastWriteOptions();
		Assert.assertNotNull("WriteOptions must have been passed to storage", usedOptions);

		// SAFETY INVARIANT: When config has logSync enabled (true), learned values MUST be synced durably (setSync(true)).
		// Currently in LearnerState, writeOptions.setSync(false) is hardcoded regardless of config.logSync(),
		// causing learned consensus logs to be written asynchronously without fsync!
		Assert.assertTrue(
				"SAFETY DEFECT #3 CONFIRMED: LearnerState.learnValue hardcoded writeOptions.setSync(false) "
						+ "despite config.logSync() == true! This risks data loss on node crash.",
				usedOptions.isSync()
		);
	}
}
