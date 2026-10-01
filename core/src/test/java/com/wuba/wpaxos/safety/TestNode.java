package com.wuba.wpaxos.safety;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.wuba.wpaxos.Instance;
import com.wuba.wpaxos.base.Base;
import com.wuba.wpaxos.base.BaseMsg;
import com.wuba.wpaxos.comm.FollowerNodeInfo;
import com.wuba.wpaxos.comm.MsgTransport;
import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.comm.enums.PaxosLogCleanType;
import com.wuba.wpaxos.comm.enums.PaxosMsgType;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.proto.PaxosMsg;
import com.wuba.wpaxos.utils.ByteConverter;

/**
 * One real WPaxos Instance (acceptor + learner + proposer) for group 0, with its
 * network replaced by a recorder. The ioLoop thread is never started: the test is the
 * scheduler, delivering each message by calling {@link Instance#onReceivePaxosMsg}
 * on the caller's thread, so every interleaving is chosen explicitly and reproducible.
 */
public class TestNode {

	public static final class Sent {
		public final long to;
		public final PaxosMsg msg;

		Sent(long to, PaxosMsg msg) {
			this.to = to;
			this.msg = msg;
		}
	}

	public final long nodeID;
	public final MemLogStorage storage;
	public final RecordingStateMachine sm = new RecordingStateMachine();
	public final List<Sent> outbox = new ArrayList<Sent>();
	public final Instance instance;

	public TestNode(NodeInfo me, List<NodeInfo> members, MemLogStorage storage) throws Exception {
		this.nodeID = me.getNodeID();
		this.storage = storage;
		Options options = new Options();
		Config config = new Config(storage, true, 0, false, me, members,
				new ArrayList<FollowerNodeInfo>(), 0, 1, null, PaxosLogCleanType.cleanByHoldCount,
				options.getLearnerSendSpeed(), null);
		config.init();
		this.instance = new Instance(config, storage, new Recorder(), options);
		this.instance.addStateMachine(sm);
		int ret = this.instance.init();
		if (ret != 0) {
			throw new IllegalStateException("Instance.init failed, ret " + ret);
		}
	}

	public static List<NodeInfo> cluster(int n) throws Exception {
		List<NodeInfo> nodes = new ArrayList<NodeInfo>();
		for (int i = 1; i <= n; i++) {
			nodes.add(new NodeInfo("127.0.0.1", 11000 + i));
		}
		return nodes;
	}

	public void deliver(PaxosMsg msg) {
		instance.onReceivePaxosMsg(msg, false);
	}

	public List<PaxosMsg> sentOfType(PaxosMsgType type) {
		List<PaxosMsg> out = new ArrayList<PaxosMsg>();
		for (Sent s : outbox) {
			if (s.msg.getMsgType() == type.getValue()) {
				out.add(s.msg);
			}
		}
		return out;
	}

	/** A value as it travels in the log: 4-byte little-endian SMID header, then the body. */
	public static byte[] packed(String body) {
		byte[] b = body.getBytes(StandardCharsets.UTF_8);
		byte[] smid = ByteConverter.intToBytesLittleEndian(RecordingStateMachine.SMID);
		byte[] out = new byte[smid.length + b.length];
		System.arraycopy(smid, 0, out, 0, smid.length);
		System.arraycopy(b, 0, out, smid.length, b.length);
		return out;
	}

	public static PaxosMsg prepare(long instanceID, long proposalID, long proposerNodeID) {
		PaxosMsg m = new PaxosMsg();
		m.setMsgType(PaxosMsgType.paxosPrepare.getValue());
		m.setInstanceID(instanceID);
		m.setNodeID(proposerNodeID);
		m.setProposalID(proposalID);
		return m;
	}

	public static PaxosMsg accept(long instanceID, long proposalID, long proposerNodeID, String value) {
		PaxosMsg m = new PaxosMsg();
		m.setMsgType(PaxosMsgType.paxosAccept.getValue());
		m.setInstanceID(instanceID);
		m.setNodeID(proposerNodeID);
		m.setProposalID(proposalID);
		m.setValue(packed(value));
		return m;
	}

	/** What a proposer broadcasts once a majority accepted its ballot. */
	public static PaxosMsg chosen(long instanceID, long proposalID, long proposerNodeID) {
		PaxosMsg m = new PaxosMsg();
		m.setMsgType(PaxosMsgType.paxosLearnerProposerSendSuccess.getValue());
		m.setInstanceID(instanceID);
		m.setNodeID(proposerNodeID);
		m.setProposalID(proposalID);
		return m;
	}

	private final class Recorder implements MsgTransport {
		@Override
		public int sendMessage(int groupIdx, long sendtoNodeID, byte[] sBuffer, int sendType) {
			outbox.add(new Sent(sendtoNodeID, decode(sBuffer)));
			return 0;
		}

		@Override
		public int broadcastMessage(int groupIdx, byte[] sBuffer, int sendType) {
			outbox.add(new Sent(-1, decode(sBuffer)));
			return 0;
		}

		@Override
		public int broadcastMessageFollower(int groupIdx, byte[] sBuffer, int sendType) {
			return 0;
		}

		@Override
		public int broadcastMessageTempNode(int groupIdx, byte[] sBuffer, int sendType) {
			return 0;
		}

		private PaxosMsg decode(byte[] buf) {
			BaseMsg base = Base.unPackBaseMsg(buf);
			return (PaxosMsg) base.getBodyProto();
		}
	}
}
