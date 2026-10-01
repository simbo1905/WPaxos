package com.wuba.wpaxos.safety;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.wuba.wpaxos.Instance;
import com.wuba.wpaxos.Learner;
import com.wuba.wpaxos.Proposer;
import com.wuba.wpaxos.base.BallotNumber;
import com.wuba.wpaxos.comm.NodeInfo;

/**
 * Safety violation: two replicas execute different values for the same instance.
 *
 * <p>{@code Instance.receiveMsgForAcceptor} has a "skip success message" shortcut (carried
 * over verbatim from phxpaxos {@code Instance::ReceiveMsgForAcceptor}): when an acceptor
 * still at instance N sees a prepare/accept for N+1, it fabricates a
 * {@code paxosLearnerProposerSendSuccess} for N carrying the <em>N+1 message's</em> ballot,
 * and the learner then treats its own accepted value at N as chosen whenever the ballots
 * match. A message for N+1 proves nothing about which value was chosen at N, and
 * {@code ProposerState.init()} keeps {@code proposalID} across instances, so a proposer
 * whose value lost instance N sends its N+1 messages with the very ballot it used at N.
 *
 * <p>Every prepare/accept below is produced by the real {@link Proposer} of n1 or n2; the
 * test only plays the network (a partition, then a heal).
 */
public class SkipSuccessLearnsUnchosenValueTest {

	@Test
	public void acceptorAtNMustNotLearnItsAcceptedValueFromAMessageForNPlusOne() throws Exception {
		List<NodeInfo> members = TestNode.cluster(5);
		final TestNode n1 = node(members, 0);
		final TestNode n2 = node(members, 1);
		final TestNode n3 = node(members, 2);
		final TestNode n4 = node(members, 3);
		final TestNode n5 = node(members, 4);
		List<TestNode> all = Arrays.asList(n1, n2, n3, n4, n5);

		// Instance 0. n1 proposes "v" with ballot (1,n1) and wins prepare with {n1,n3,n4}.
		proposer(n1).newValue(TestNode.packed("v"));
		route(n1, all, n3, n4);
		route(n3, all, n1);
		route(n4, all, n1);

		// Partition {n1,n3} | {n2,n4,n5}: n1's accept reaches only n3. n1 sees no reject.
		route(n1, all, n3);
		route(n3, all, n1);

		// n2 proposes "w" on the majority side; nobody there accepted anything, so "w" is chosen.
		proposer(n2).newValue(TestNode.packed("w"));
		route(n2, all, n4, n5);
		route(n4, all, n2);
		route(n5, all, n2);
		route(n2, all, n4, n5); // accept
		route(n4, all, n2);
		route(n5, all, n2);
		route(n2, all, n4, n5); // chosen

		assertEquals("w", n2.sm.executed().get(0L));
		assertEquals("w", n4.sm.executed().get(0L));
		assertEquals("w", n5.sm.executed().get(0L));

		// Heal. n1 catches up on instance 0 through ordinary learner catch-up from n2.
		learner(n2).sendLearnValue(n1.nodeID, 0, new BallotNumber(1, n2.nodeID), TestNode.packed("w"), 0, false);
		route(n2, all, n1);
		assertEquals("w", n1.sm.executed().get(0L));

		// n1 proposes its next value at instance 1 -- with ballot (1,n1), unchanged, because
		// nothing ever rejected it. n3 has not caught up yet and is still at instance 0.
		proposer(n1).newValue(TestNode.packed("x"));
		route(n1, all, n3);

		// "w" was chosen at instance 0. n3 may not have learned it yet, but it must not have
		// executed anything else there.
		String atZero = n3.sm.executed().get(0L);
		assertTrue("replica n3 executed \"" + atZero + "\" at instance 0, which was never chosen; "
				+ "n1,n2,n4,n5 executed \"w\"", atZero == null || atZero.equals("w"));
	}

	private static TestNode node(List<NodeInfo> members, int i) throws Exception {
		return new TestNode(members.get(i), members, new MemLogStorage());
	}

	/** Delivers everything {@code from} has sent to the reachable nodes, then empties its outbox. */
	private static void route(TestNode from, List<TestNode> all, TestNode... reachable) {
		Set<Long> ok = new HashSet<Long>();
		for (TestNode t : reachable) {
			ok.add(t.nodeID);
		}
		List<TestNode.Sent> batch = new ArrayList<TestNode.Sent>(from.outbox);
		from.outbox.clear();
		for (TestNode.Sent s : batch) {
			for (TestNode t : all) {
				if (t == from || !ok.contains(t.nodeID)) {
					continue;
				}
				if (s.to == -1 || s.to == t.nodeID) {
					t.deliver(s.msg);
				}
			}
		}
	}

	static Proposer proposer(TestNode n) throws Exception {
		return (Proposer) field(n.instance, "proposer");
	}

	static Learner learner(TestNode n) throws Exception {
		return (Learner) field(n.instance, "learner");
	}

	private static Object field(Instance instance, String name) throws Exception {
		Field f = Instance.class.getDeclaredField(name);
		f.setAccessible(true);
		return f.get(instance);
	}
}
