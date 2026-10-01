package com.wuba.wpaxos.safety;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.file.Files;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.wuba.wpaxos.comm.InsideOptions;
import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.comm.enums.IndexType;
import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.proto.AcceptorStateData;
import com.wuba.wpaxos.store.DefaultLogStorage;
import com.wuba.wpaxos.store.PaxosLog;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

/**
 * Safety violation: {@code DefaultLogStorage.put} ignores the int returned by
 * {@code DefaultDataBase.put} and returns 0 unless an exception escapes, so every
 * non-exceptional store failure reaches {@code AcceptorState.persist} as success and the
 * acceptor promises/accepts with nothing on disk.
 *
 * <p>No I/O fault is needed to hit it: the propose path admits values up to
 * {@code InsideOptions.getMaxBufferSize()} (10 MiB), and {@code StoreConfig.maxMessageSize}
 * is the same 10 MiB, but the stored record is the value plus the AcceptorStateData and
 * append headers. A maximum-size value is therefore always rejected by PhysicLog with
 * MESSAGE_SIZE_EXCEEDED -- and reported upward as written.
 */
public class LogStoragePutSwallowsFailureTest {

	private File dir;
	private DefaultLogStorage storage;

	@Before
	public void setUp() throws Exception {
		dir = Files.createTempDirectory("wpaxos-store").toFile();
		Constructor<DefaultLogStorage> c = DefaultLogStorage.class.getDeclaredConstructor();
		c.setAccessible(true);
		storage = c.newInstance();
		Options options = new Options();
		options.setLogStoragePath(dir.getAbsolutePath());
		options.setGroupCount(1);
		options.setIndexType(IndexType.PHYSIC_FILE);
		assertTrue("precondition: store init", storage.init(options));
	}

	@After
	public void tearDown() {
		storage.shutdown();
		deleteRecursively(dir);
	}

	@Test
	public void aWriteThatReportsSuccessMustBeReadable() throws Exception {
		byte[] value = new byte[InsideOptions.getInstance().getMaxBufferSize()];
		AcceptorStateData state = new AcceptorStateData();
		state.setInstanceID(0);
		state.setPromiseID(1);
		state.setPromiseNodeID(1);
		state.setAcceptedID(1);
		state.setAcceptedNodeID(1);
		state.setAcceptedValue(value);

		PaxosLog paxosLog = new PaxosLog(storage);
		int ret = paxosLog.writeState(new WriteOptions(true), 0, 0, state);

		JavaOriTypeWrapper<byte[]> read = new JavaOriTypeWrapper<byte[]>();
		int getRet = storage.get(0, 0, read);
		assertTrue("LogStorage.put returned " + ret + " (success) for an accept record that was never "
				+ "stored (get returned " + getRet + "); AcceptorState.persist would ack it",
				ret != 0 || getRet == 0);
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
