package com.wuba.wpaxos.safety;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.comm.enums.PaxosMsgType;
import com.wuba.wpaxos.proto.AcceptorStateData;
import com.wuba.wpaxos.proto.PaxosMsg;

/**
 * Safety violations caused by {@code Acceptor.onAccept} when {@code AcceptorState.persist}
 * fails.
 *
 * <p>{@code updateAcceptorState4Accept} returns void, so the accept reply is still sent with
 * {@code rejectByPromiseID == 0} and the proposer counts it as a vote. (phxpaxos returns
 * before replying.) Separately, the in-memory accepted ballot/value were overwritten before
 * the persist and are not rolled back, so the learner can later "learn" a value this
 * acceptor never made durable, and a restart replays whatever older record is on disk.
 * Suppressing the reply alone does not fix the second test.
 */
public class AcceptorPersistFailureTest {

	@Test
	public void acceptReplyMustNotCountAsAVoteWhenPersistFailed() throws Exception {
		List<NodeInfo> members = TestNode.cluster(3);
		long n1 = members.get(0).getNodeID();
		MemLogStorage storage = new MemLogStorage();
		TestNode x = new TestNode(members.get(2), members, storage);

		x.deliver(TestNode.prepare(0, 1, n1));
		storage.setFault(new MemLogStorage.PutFault() {
			@Override
			public boolean fail(long instanceID, AcceptorStateData state) {
				return state.getAcceptedID() != 0;
			}
		});
		x.deliver(TestNode.accept(0, 1, n1, "v"));

		assertEquals("precondition: nothing was persisted for the accept", 0, storage.read(0).getAcceptedID());
		List<PaxosMsg> replies = x.sentOfType(PaxosMsgType.paxosAcceptReply);
		for (PaxosMsg r : replies) {
			assertTrue("acceptor failed to persist the accepted value but replied as if it had accepted it; "
					+ "the proposer counts this toward a quorum", r.getRejectByPromiseID() != 0);
		}
	}

	@Test
	public void restartMustNotReplayAValueThatWasNeverChosen() throws Exception {
		List<NodeInfo> members = TestNode.cluster(3);
		long n1 = members.get(0).getNodeID();
		long n2 = members.get(1).getNodeID();
		MemLogStorage storage = new MemLogStorage();
		TestNode x = new TestNode(members.get(2), members, storage);

		// n1 gets "v" accepted by x only (n1 crashes before reaching anyone else). Durable.
		x.deliver(TestNode.prepare(0, 1, n1));
		x.deliver(TestNode.accept(0, 1, n1, "v"));

		// n2 runs a full round. x's prepare reply reports "v", but n2's prepare quorum {n1,n2}
		// had accepted nothing, so n2 proposes "w". x's persist of "w" fails.
		x.deliver(TestNode.prepare(0, 2, n2));
		storage.setFault(new MemLogStorage.PutFault() {
			@Override
			public boolean fail(long instanceID, AcceptorStateData state) {
				return state.getAcceptedID() == 2;
			}
		});
		x.deliver(TestNode.accept(0, 2, n2, "w"));
		storage.setFault(null);

		// "w" is chosen ({n1,n2} plus x's reply), and x learns it from the chosen broadcast.
		x.deliver(TestNode.chosen(0, 2, n2));
		assertEquals("precondition: x executed the chosen value", "w", x.sm.executed().get(0L));

		// Life goes on at instance 1, which gives x a log record past instance 0.
		x.deliver(TestNode.prepare(1, 2, n2));
		x.deliver(TestNode.accept(1, 2, n2, "y"));

		// x restarts on the same log and replays instance 0.
		TestNode restarted = new TestNode(members.get(2), members, storage);
		assertEquals("after restart x replayed a value for instance 0 that was never chosen "
				+ "(before the restart it had executed \"w\")", "w", restarted.sm.executed().get(0L));
	}
}
