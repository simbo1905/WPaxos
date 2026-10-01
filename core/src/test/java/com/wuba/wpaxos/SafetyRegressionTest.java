/*
 * Copyright (C) 2005-present, 58.com. All rights reserved.
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
package com.wuba.wpaxos;

import com.wuba.wpaxos.base.BallotNumber;
import com.wuba.wpaxos.comm.FollowerNodeInfo;
import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.comm.enums.PaxosLogCleanType;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.config.WriteState;
import com.wuba.wpaxos.helper.LearnerState;
import com.wuba.wpaxos.proto.PaxosMsg;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.store.pagecache.MapedFile;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Executable proofs for safety defects found by the durability audit.
 *
 * These tests intentionally fail against the vulnerable implementation. They
 * encode the required safety properties so each production fix can make its
 * corresponding test pass.
 */
public class SafetyRegressionTest {

    @Test
    public void acceptMustNotReplySuccessWhenPersistenceFails() {
        RecordingLogStorage storage = new RecordingLogStorage();
        Config config = config(storage, true);
        CapturingAcceptor acceptor = new CapturingAcceptor(config, storage);
        acceptor.setAcceptorState(new PersistFailingAcceptorState(config, storage));

        PaxosMsg request = new PaxosMsg();
        request.setInstanceID(7);
        request.setNodeID(2);
        request.setProposalID(11);
        request.setValue(new byte[] {42});

        acceptor.onAccept(request);

        PaxosMsg reply = acceptor.sentReply;
        assertTrue(
                "an acceptor must not emit a successful accept reply after its durable write failed",
                reply == null || reply.getRejectByPromiseID() != 0);
    }

    @Test
    public void learnedValueMustUseSynchronousPersistenceWhenLogSyncIsEnabled() {
        RecordingLogStorage storage = new RecordingLogStorage();
        LearnerState learnerState = new LearnerState(config(storage, true), storage);

        int result = learnerState.learnValue(
                7, new BallotNumber(11, 2), new byte[] {42}, 0);

        assertEquals(0, result);
        assertNotNull("the learner did not write the learned value", storage.lastWriteOptions);
        assertTrue(
                "a successful learn must not remain vulnerable to acknowledged crash loss",
                storage.lastWriteOptions.isSync());
    }

    @Test
    public void failedForceMustNotAdvanceFlushedPosition() throws Exception {
        File directory = Files.createTempDirectory("wpaxos-force-failure").toFile();
        File file = new File(directory, "0");
        MapedFile mapedFile = new MapedFile(file.getAbsolutePath(), 4096, true);
        try {
            assertTrue(mapedFile.appendData(new byte[] {1, 2, 3, 4}));
            mapedFile.getFileChannel().close(); // deterministic force-path failure

            int flushedPosition = mapedFile.flush(0);

            assertEquals(
                    "a failed force must leave the bytes dirty so a later flush can retry",
                    0, flushedPosition);
        } finally {
            mapedFile.destroy(0);
            file.delete();
            directory.delete();
        }
    }

    private static Config config(LogStorage storage, boolean logSync) {
        NodeInfo node = new NodeInfo();
        node.setNodeID(1);
        return new Config(
                storage,
                logSync,
                0,
                false,
                node,
                Collections.singletonList(node),
                Collections.<FollowerNodeInfo>emptyList(),
                0,
                1,
                null,
                PaxosLogCleanType.cleanByHoldCount,
                0,
                null);
    }

    private static final class CapturingAcceptor extends Acceptor {
        private PaxosMsg sentReply;

        private CapturingAcceptor(Config config, LogStorage storage) {
            super(config, null, null, storage);
        }

        @Override
        public int getLastChecksum() {
            return 0;
        }

        @Override
        public int sendMessage(long toNodeID, PaxosMsg paxosMsg) {
            this.sentReply = paxosMsg;
            return 0;
        }
    }

    private static final class PersistFailingAcceptorState extends AcceptorState {
        private PersistFailingAcceptorState(Config config, LogStorage storage) {
            super(config, storage);
        }

        @Override
        public int persist(long instanceID, int lastChecksum) {
            return -1;
        }
    }

    private static final class RecordingLogStorage implements LogStorage {
        private WriteOptions lastWriteOptions;

        @Override
        public boolean init(Options option) {
            return true;
        }

        @Override
        public String getLogStorageDirPath(int groupIdx) {
            return null;
        }

        @Override
        public int get(int groupIdx, long instanceID, JavaOriTypeWrapper<byte[]> valueWrap) {
            return 1;
        }

        @Override
        public int put(
                WriteOptions writeOptions,
                int groupIdx,
                long instanceID,
                byte[] value,
                WriteState writeState) {
            this.lastWriteOptions = writeOptions;
            return 0;
        }

        @Override
        public int delOne(WriteOptions writeOptions, int groupIdx, long instanceID) {
            return 0;
        }

        @Override
        public int delExpire(WriteOptions writeOptions, int groupIdx, long maxInstanceId) {
            return 0;
        }

        @Override
        public int getMaxInstanceID(int groupIdx, JavaOriTypeWrapper<Long> instanceID) {
            return 1;
        }

        @Override
        public int setMinChosenInstanceID(
                WriteOptions writeOptions, int groupIdx, long minInstanceID) {
            return 0;
        }

        @Override
        public long getMinChosenInstanceID(int groupIdx) {
            return 0;
        }

        @Override
        public int clearAllLog(int groupIdx) {
            return 0;
        }

        @Override
        public int setSystemVariables(
                WriteOptions writeOptions, int groupIdx, byte[] buffer) {
            return 0;
        }

        @Override
        public byte[] getSystemVariables(int groupIdx) {
            return null;
        }

        @Override
        public int setMasterVariables(
                WriteOptions writeOptions, int groupIdx, byte[] buffer) {
            return 0;
        }

        @Override
        public byte[] getMasterVariables(int groupIdx) {
            return null;
        }

        @Override
        public void start() {
        }

        @Override
        public void shutdown() {
        }

        @Override
        public void deleteOneIndex(int groupId, long instanceId) {
        }

        @Override
        public void deleteExpireIndex(int groupId, long maxInstanceId) {
        }

        @Override
        public boolean isAvailable(int groupId) {
            return true;
        }
    }
}
