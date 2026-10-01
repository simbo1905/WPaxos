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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.wuba.wpaxos.Instance;
import com.wuba.wpaxos.base.Base;
import com.wuba.wpaxos.base.BaseMsg;
import com.wuba.wpaxos.comm.FollowerNodeInfo;
import com.wuba.wpaxos.comm.MsgTransport;
import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.comm.enums.PaxosLogCleanType;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.proto.PaxosMsg;
import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.storemachine.StateMachine;
import com.wuba.wpaxos.utils.ByteConverter;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

public class TestSupport {

	public static class TestMsgTransport implements MsgTransport {
		private final List<PaxosMsg> sentMessages = new CopyOnWriteArrayList<>();

		public List<PaxosMsg> getSentMessages() {
			return sentMessages;
		}

		public PaxosMsg getLastSentMessage() {
			if (sentMessages.isEmpty()) {
				return null;
			}
			return sentMessages.get(sentMessages.size() - 1);
		}

		public void clear() {
			sentMessages.clear();
		}

		@Override
		public int sendMessage(int groupIdx, long sendtoNodeID, byte[] sBuffer, int sendType) {
			BaseMsg baseMsg = Base.unPackBaseMsg(sBuffer);
			if (baseMsg != null && baseMsg.getBodyProto() instanceof PaxosMsg) {
				sentMessages.add((PaxosMsg) baseMsg.getBodyProto());
			}
			return 0;
		}

		@Override
		public int broadcastMessage(int groupIdx, byte[] sBuffer, int sendType) {
			BaseMsg baseMsg = Base.unPackBaseMsg(sBuffer);
			if (baseMsg != null && baseMsg.getBodyProto() instanceof PaxosMsg) {
				sentMessages.add((PaxosMsg) baseMsg.getBodyProto());
			}
			return 0;
		}

		@Override
		public int broadcastMessageFollower(int groupIdx, byte[] sBuffer, int sendType) {
			return broadcastMessage(groupIdx, sBuffer, sendType);
		}

		@Override
		public int broadcastMessageTempNode(int groupIdx, byte[] sBuffer, int sendType) {
			return broadcastMessage(groupIdx, sBuffer, sendType);
		}
	}

	public static class TestStateMachine implements StateMachine {
		public static final int SMID = 100;
		private final Map<Long, byte[]> executed = new ConcurrentHashMap<>();

		@Override
		public int getSMID() {
			return SMID;
		}

		@Override
		public boolean execute(int groupIdx, long instanceID, byte[] paxosValue, SMCtx smCtx) {
			executed.put(instanceID, paxosValue);
			return true;
		}

		@Override
		public boolean executeForCheckpoint(int groupIdx, long instanceID, byte[] paxosValue) {
			return true;
		}

		@Override
		public long getCheckpointInstanceID(int groupIdx) {
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
			return null;
		}

		@Override
		public boolean needCallBeforePropose() {
			return false;
		}

		@Override
		public void fixCheckpointByMinChosenInstanceId(long minChosenInstanceID) {
		}

		public boolean isExecuted(long instanceID) {
			return executed.containsKey(instanceID);
		}

		public byte[] getExecutedValue(long instanceID) {
			return executed.get(instanceID);
		}

		public Map<Long, byte[]> getExecuted() {
			return executed;
		}
	}

	public static byte[] packValue(int smID, byte[] value) {
		byte[] smIDbuf = ByteConverter.intToBytesLittleEndian(smID);
		byte[] buf = new byte[smIDbuf.length + value.length];
		System.arraycopy(smIDbuf, 0, buf, 0, smIDbuf.length);
		System.arraycopy(value, 0, buf, smIDbuf.length, value.length);
		return buf;
	}

	public static Config createConfig(MemoryLogStorage logStorage, boolean writeSync) throws Exception {
		NodeInfo myNode = new NodeInfo("127.0.0.1", 20001);
		List<NodeInfo> nodeList = new ArrayList<>();
		nodeList.add(myNode);
		nodeList.add(new NodeInfo("127.0.0.1", 20002));
		nodeList.add(new NodeInfo("127.0.0.1", 20003));

		List<FollowerNodeInfo> followerList = new ArrayList<>();

		Config config = new Config(
				logStorage,
				writeSync,
				0, // syncInterval
				false, // useMembership
				myNode,
				nodeList,
				followerList,
				0, // groupIdx
				1, // groupCount
				null,
				PaxosLogCleanType.cleanByHoldCount,
				1000,
				null
		);
		return config;
	}

	public static Options createOptions() throws Exception {
		Options options = new Options();
		options.setGroupCount(1);
		NodeInfo myNode = new NodeInfo("127.0.0.1", 20001);
		options.setMyNode(myNode);
		ArrayList<NodeInfo> list = new ArrayList<>();
		list.add(myNode);
		options.setNodeInfoList(list);
		Map<Integer, ArrayList<NodeInfo>> map = new HashMap<>();
		map.put(0, list);
		options.setNodeInfoMap(map);
		options.setWriteSync(true);
		options.setUseCheckpointReplayer(false);
		options.setCommitTimeout(1000);
		return options;
	}

	public static Instance createInstance(Config config, MemoryLogStorage logStorage, MsgTransport transport, Options options) {
		return new Instance(config, logStorage, transport, options);
	}
}
