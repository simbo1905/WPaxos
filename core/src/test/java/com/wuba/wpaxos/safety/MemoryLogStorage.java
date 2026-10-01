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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.config.WriteState;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

/**
 * In-memory LogStorage implementation for deterministic safety testing.
 */
public class MemoryLogStorage implements LogStorage {
	private final Map<Integer, ConcurrentSkipListMap<Long, byte[]>> groupLogs = new ConcurrentHashMap<>();
	private final Map<Integer, Long> minChosenMap = new ConcurrentHashMap<>();
	private final Map<Integer, byte[]> systemVars = new ConcurrentHashMap<>();
	private final Map<Integer, byte[]> masterVars = new ConcurrentHashMap<>();
	private volatile boolean failPut = false;
	private volatile WriteOptions lastWriteOptions;

	private ConcurrentSkipListMap<Long, byte[]> getMap(int groupIdx) {
		return groupLogs.computeIfAbsent(groupIdx, k -> new ConcurrentSkipListMap<>());
	}

	public void setFailPut(boolean failPut) {
		this.failPut = failPut;
	}

	public WriteOptions getLastWriteOptions() {
		return lastWriteOptions;
	}

	@Override
	public boolean init(Options option) throws Exception {
		return true;
	}

	@Override
	public String getLogStorageDirPath(int groupIdx) {
		return "/tmp/wpaxos_memory_log";
	}

	@Override
	public int get(int groupIdx, long instanceID, JavaOriTypeWrapper<byte[]> valueWrap) {
		byte[] val = getMap(groupIdx).get(instanceID);
		if (val == null) {
			return 1;
		}
		valueWrap.setValue(val);
		return 0;
	}

	@Override
	public int put(WriteOptions writeOptions, int groupIdx, long instanceID, byte[] sValue, WriteState writeState) {
		this.lastWriteOptions = writeOptions;
		if (failPut) {
			return -1;
		}
		getMap(groupIdx).put(instanceID, sValue);
		return 0;
	}

	@Override
	public int delOne(WriteOptions writeOptions, int groupIdx, long instanceID) {
		getMap(groupIdx).remove(instanceID);
		return 0;
	}

	@Override
	public int delExpire(WriteOptions writeOptions, int groupIdx, long maxInstanceId) {
		getMap(groupIdx).headMap(maxInstanceId).clear();
		return 0;
	}

	@Override
	public int getMaxInstanceID(int groupIdx, JavaOriTypeWrapper<Long> instanceID) {
		ConcurrentSkipListMap<Long, byte[]> map = getMap(groupIdx);
		if (map.isEmpty()) {
			return 1;
		}
		instanceID.setValue(map.lastKey());
		return 0;
	}

	@Override
	public int setMinChosenInstanceID(WriteOptions writeOptions, int groupIdx, long minInstanceID) {
		minChosenMap.put(groupIdx, minInstanceID);
		return 0;
	}

	@Override
	public long getMinChosenInstanceID(int groupIdx) {
		Long val = minChosenMap.get(groupIdx);
		return val != null ? val : 0L;
	}

	@Override
	public int clearAllLog(int groupIdx) {
		getMap(groupIdx).clear();
		return 0;
	}

	@Override
	public int setSystemVariables(WriteOptions writeOptions, int groupIdx, byte[] buffer) {
		systemVars.put(groupIdx, buffer);
		return 0;
	}

	@Override
	public byte[] getSystemVariables(int groupIdx) {
		return systemVars.get(groupIdx);
	}

	@Override
	public int setMasterVariables(WriteOptions writeOptions, int groupIdx, byte[] buffer) {
		masterVars.put(groupIdx, buffer);
		return 0;
	}

	@Override
	public byte[] getMasterVariables(int groupIdx) {
		return masterVars.get(groupIdx);
	}

	@Override
	public void start() {
	}

	@Override
	public void shutdown() {
	}

	@Override
	public void deleteOneIndex(int groupId, long instanceId) {
		delOne(null, groupId, instanceId);
	}

	@Override
	public void deleteExpireIndex(int groupId, long maxInstanceId) {
		delExpire(null, groupId, maxInstanceId);
	}

	@Override
	public boolean isAvailable(int groupId) {
		return true;
	}
}
