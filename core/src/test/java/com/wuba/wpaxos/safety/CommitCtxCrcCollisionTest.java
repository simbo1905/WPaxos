package com.wuba.wpaxos.safety;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

import com.wuba.wpaxos.CommitCtx;
import com.wuba.wpaxos.config.PaxosTryCommitRet;
import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.utils.Crc32;

/**
 * Safety violation: a client is told its value was committed when a different value was
 * chosen.
 *
 * <p>{@code CommitCtx.isMycommit} and {@code CommitCtx.setResult} decide "the chosen value
 * is the one I proposed" by comparing length and {@code Crc32.crc32}, which is CRC-32
 * masked to 31 bits. phxpaxos compares the bytes. The two lin-kv operations below (the
 * encoding the Maelstrom harness proposes) were found by a birthday search over a few
 * thousand random writes: same length, same masked CRC, different key and value.
 */
public class CommitCtxCrcCollisionTest {

	private static final String PROPOSED = "{\"k\":1,\"key\":\"387\",\"value\":616168}";
	private static final String CHOSEN = "{\"k\":1,\"key\":\"835\",\"value\":907599}";

	@Test
	public void aDifferentChosenValueMustBeReportedAsAConflict() {
		byte[] proposed = TestNode.packed(PROPOSED);
		byte[] chosen = TestNode.packed(CHOSEN);
		assertFalse(Arrays.equals(proposed, chosen));
		assertEquals("precondition: same length", proposed.length, chosen.length);
		assertEquals("precondition: same masked CRC", Crc32.crc32(proposed), Crc32.crc32(chosen));

		Object clientWaiting = new Object();
		CommitCtx ctx = new CommitCtx(null);
		ctx.newCommit(proposed, new SMCtx(RecordingStateMachine.SMID, clientWaiting), 1000);
		ctx.startCommit(5);

		SMCtx execCtx = new SMCtx();
		boolean mine = ctx.isMycommit(5, chosen, execCtx);
		ctx.setResult(PaxosTryCommitRet.PaxosTryCommitRet_OK.getRet(), 5, chosen);
		int ret = ctx.getResult().getCommitRet();

		assertTrue("the waiting client's context was handed to the execution of someone else's value",
				!mine && execCtx.getpCtx() == null);
		assertNotEquals("propose of " + PROPOSED + " returned OK, but instance 5 chose " + CHOSEN,
				PaxosTryCommitRet.PaxosTryCommitRet_OK.getRet(), ret);
	}
}
