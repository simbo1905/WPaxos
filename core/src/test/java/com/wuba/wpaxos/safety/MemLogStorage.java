package com.wuba.wpaxos.safety;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.config.WriteState;
import com.wuba.wpaxos.proto.AcceptorStateData;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

/**
 * Single-group, in-memory paxos log. It survives a "restart" because the same object
 * can be handed to a fresh Instance, which is all a crash/restart test needs.
 */
public class MemLogStorage implements LogStorage {

	public interface PutFault {
		/** @return true to make this put fail without storing anything. */
		boolean fail(long instanceID, AcceptorStateData state);
	}

	public static final class Put {
		public final long instanceID;
		public final boolean sync;
		public final AcceptorStateData state;

		Put(long instanceID, boolean sync, AcceptorStateData state) {
			this.instanceID = instanceID;
			this.sync = sync;
			this.state = state;
		}
	}

	private final TreeMap<Long, byte[]> log = new TreeMap<Long, byte[]>();
	private final List<Put> puts = new ArrayList<Put>();
	private byte[] systemVariables;
	private byte[] masterVariables;
	private long minChosen;
	private volatile PutFault fault;

	public void setFault(PutFault fault) {
		this.fault = fault;
	}

	public List<Put> puts() {
		return puts;
	}

	public AcceptorStateData read(long instanceID) throws Exception {
		byte[] b = log.get(instanceID);
		if (b == null) {
			return null;
		}
		AcceptorStateData s = new AcceptorStateData();
		s.parseFromBytes(b, b.length);
		return s;
	}

	@Override
	public boolean init(Options option) {
		return true;
	}

	@Override
	public String getLogStorageDirPath(int groupIdx) {
		return null;
	}

	@Override
	public synchronized int get(int groupIdx, long instanceID, JavaOriTypeWrapper<byte[]> valueWrap) {
		byte[] b = log.get(instanceID);
		if (b == null) {
			return 1;
		}
		valueWrap.setValue(b);
		return 0;
	}

	@Override
	public synchronized int put(WriteOptions writeOptions, int groupIdx, long instanceID, byte[] sValue, WriteState writeState) {
		AcceptorStateData state = new AcceptorStateData();
		try {
			state.parseFromBytes(sValue, sValue.length);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		puts.add(new Put(instanceID, writeOptions.isSync(), state));
		PutFault f = fault;
		if (f != null && f.fail(instanceID, state)) {
			return -1;
		}
		log.put(instanceID, sValue);
		return 0;
	}

	@Override
	public int delOne(WriteOptions writeOptions, int groupIdx, long instanceID) {
		log.remove(instanceID);
		return 0;
	}

	@Override
	public int delExpire(WriteOptions writeOptions, int groupIdx, long maxInstanceId) {
		return 0;
	}

	@Override
	public synchronized int getMaxInstanceID(int groupIdx, JavaOriTypeWrapper<Long> instanceID) {
		if (log.isEmpty()) {
			instanceID.setValue(0L);
			return 1;
		}
		instanceID.setValue(log.lastKey());
		return 0;
	}

	@Override
	public int setMinChosenInstanceID(WriteOptions writeOptions, int groupIdx, long minInstanceID) {
		this.minChosen = minInstanceID;
		return 0;
	}

	@Override
	public long getMinChosenInstanceID(int groupIdx) {
		return minChosen;
	}

	@Override
	public int clearAllLog(int groupIdx) {
		log.clear();
		return 0;
	}

	@Override
	public int setSystemVariables(WriteOptions writeOptions, int groupIdx, byte[] buffer) {
		this.systemVariables = buffer;
		return 0;
	}

	@Override
	public byte[] getSystemVariables(int groupIdx) {
		return systemVariables;
	}

	@Override
	public int setMasterVariables(WriteOptions writeOptions, int groupIdx, byte[] buffer) {
		this.masterVariables = buffer;
		return 0;
	}

	@Override
	public byte[] getMasterVariables(int groupIdx) {
		return masterVariables;
	}

	@Override
	public void start() {
	}

	@Override
	public void shutdown() {
	}

	@Override
	public void deleteOneIndex(int groupId, long instanceId) {
		log.remove(instanceId);
	}

	@Override
	public void deleteExpireIndex(int groupId, long maxInstanceId) {
	}

	@Override
	public boolean isAvailable(int groupId) {
		return true;
	}
}
