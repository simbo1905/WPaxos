package com.wuba.wpaxos.safety;

import static org.junit.Assert.assertEquals;

import java.util.TreeSet;

import org.junit.Test;

import com.wuba.wpaxos.proto.PaxosNodeInfo;
import com.wuba.wpaxos.proto.SystemVariables;
import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.storemachine.SystemVSM;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

/**
 * Safety violation: the membership state machine is not deterministic.
 *
 * <p>{@code SystemVSM.execute} rejects a membership change whose gid/version is stale with
 * {@code return true} -- but only inside {@code if (smret != null)}, i.e. only on the node
 * that proposed it and is waiting for the answer. Every other replica, and the proposer
 * itself when it replays its log after a restart, falls through and applies the stale
 * change. phxpaxos returns unconditionally. Replicas that disagree on membership disagree
 * on what a majority is.
 */
public class SystemVSMNonDeterminismTest {

	private static final long GID = 42;

	@Test
	public void aChosenMembershipChangeMustHaveTheSameEffectOnEveryReplica() throws Exception {
		SystemVSM proposer = vsm(1);
		SystemVSM other = vsm(2);

		// Both changes are built from version 10, as two concurrent PNode.addMember calls are.
		byte[] addNode4 = change(10, 1, 2, 3, 4);
		byte[] addNode5 = change(10, 1, 2, 3, 5);

		// Instance 15 chose addNode4 (proposed elsewhere); instance 16 chose the now-stale addNode5,
		// which node 1 proposed and is waiting on.
		proposer.execute(0, 15, addNode4, null);
		other.execute(0, 15, addNode4, null);

		JavaOriTypeWrapper<Integer> answer = new JavaOriTypeWrapper<Integer>(-1);
		proposer.execute(0, 16, addNode5, new SMCtx(proposer.getSMID(), answer));
		other.execute(0, 16, addNode5, null);

		assertEquals("replicas executed the same log but ended with different memberships",
				new TreeSet<Long>(proposer.getMembershipMap()), new TreeSet<Long>(other.getMembershipMap()));
	}

	private static SystemVSM vsm(long myNodeID) throws Exception {
		SystemVSM vsm = new SystemVSM(0, myNodeID, new MemLogStorage(), null, null);
		vsm.init();
		SystemVariables initial = variables(10, 1, 2, 3);
		assertEquals(0, vsm.updateSystemVariables(initial));
		return vsm;
	}

	private static byte[] change(long version, long... members) throws Exception {
		return variables(version, members).serializeToBytes();
	}

	private static SystemVariables variables(long version, long... members) {
		SystemVariables v = new SystemVariables();
		v.setGid(GID);
		v.setVersion(version);
		for (long m : members) {
			PaxosNodeInfo n = new PaxosNodeInfo();
			n.setRid(0);
			n.setNodeID(m);
			v.addMemberShip(n);
		}
		return v;
	}
}
