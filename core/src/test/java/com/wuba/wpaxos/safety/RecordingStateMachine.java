package com.wuba.wpaxos.safety;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.TreeMap;

import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.storemachine.StateMachine;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

/** Records which value each instance executed, which is exactly what replicas must agree on. */
public class RecordingStateMachine implements StateMachine {

	public static final int SMID = 7;

	private final TreeMap<Long, String> executed = new TreeMap<Long, String>();

	public synchronized TreeMap<Long, String> executed() {
		return new TreeMap<Long, String>(executed);
	}

	@Override
	public int getSMID() {
		return SMID;
	}

	@Override
	public synchronized boolean execute(int groupIdx, long instanceID, byte[] paxosValue, SMCtx smCtx) {
		executed.put(instanceID, new String(paxosValue, StandardCharsets.UTF_8));
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
	public int loadCheckpointState(int groupIdx, String checkpointTmpFileDirPath, List<String> fileList, long checkpointInstanceID) {
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
}
