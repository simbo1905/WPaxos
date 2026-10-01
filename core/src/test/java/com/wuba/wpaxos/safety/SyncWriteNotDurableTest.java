package com.wuba.wpaxos.safety;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Files;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.config.WriteState;
import com.wuba.wpaxos.store.AppendDataResult;
import com.wuba.wpaxos.store.PhysicLog;
import com.wuba.wpaxos.store.PutDataResult;
import com.wuba.wpaxos.store.PutDataStatus;
import com.wuba.wpaxos.store.config.StoreConfig;
import com.wuba.wpaxos.store.pagecache.MapedFile;

/**
 * Two ways a write the acceptor asked to be synced is acknowledged without being forced
 * to disk. Both break the acceptor's promise/accept durability that Paxos safety depends on
 * across a power loss.
 */
public class SyncWriteNotDurableTest {

	private File dir;

	@Before
	public void setUp() throws Exception {
		dir = Files.createTempDirectory("wpaxos-flush").toFile();
	}

	@After
	public void tearDown() {
		deleteRecursively(dir);
	}

	/**
	 * {@code MapedFile.flush} wraps {@code force()} in {@code catch (Throwable)} and then
	 * advances {@code flushedPosition} anyway, so a failed fsync is recorded as done. The
	 * channel is closed here only to make {@code force()} throw deterministically.
	 */
	@Test
	public void failedForceMustNotAdvanceFlushedPosition() throws Exception {
		MapedFile file = new MapedFile(new File(dir, "00000000000000000000").getPath(), 4096, true);
		assertTrue(file.appendData(new byte[128]));

		Field channel = MapedFile.class.getDeclaredField("fileChannel");
		channel.setAccessible(true);
		((FileChannel) channel.get(file)).close();

		int flushed = file.flush(0);
		assertEquals("force() threw, yet MapedFile reports 128 bytes as flushed", 0, flushed);
	}

	/**
	 * {@code PhysicLog.appendData} flushes right after the first append attempt. When that
	 * attempt returns END_OF_FILE, the record is re-appended into a new mapped file and the
	 * method returns PUT_OK without flushing again, so the record that rolled over is not
	 * durable even though {@code WriteOptions.sync} was true.
	 */
	@Test
	public void syncAppendThatRollsOverToANewFileMustBeFlushed() throws Exception {
		StoreConfig config = new StoreConfig(dir.getPath(), null);
		config.setMapedFileSizePhysic(4096);
		PhysicLog log = new PhysicLog(null, 0, new File(dir, "physic").getPath(), config, null);
		assertTrue(log.load());

		byte[] record = new byte[1000];
		for (int i = 0; i < 8; i++) {
			PutDataResult put = log.appendData(new WriteOptions(true), i, record, new WriteState());
			assertEquals(PutDataStatus.PUT_OK, put.getPutDataStatus());
			AppendDataResult r = put.getAppendDataResult();
			long end = r.getWroteOffset() + r.getWroteBytes();
			MapedFile holder = log.getMapedFileQueue().findMapedFileByOffset(r.getWroteOffset(), false);
			long flushedUpTo = holder.getFileFromOffset() + holder.getFlushedPosition();
			assertTrue("sync append #" + i + " at [" + r.getWroteOffset() + ", " + end
					+ ") returned PUT_OK but its file is only flushed up to " + flushedUpTo,
					flushedUpTo >= end);
		}
	}

	private static void deleteRecursively(File f) {
		File[] children = f.listFiles();
		if (children != null) {
			for (File c : children) {
				deleteRecursively(c);
			}
		}
		f.delete();
	}
}
