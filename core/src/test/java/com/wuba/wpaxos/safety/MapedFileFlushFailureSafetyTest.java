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

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.wuba.wpaxos.store.pagecache.MapedFile;

/**
 * Proof of Defect #2 in ticket 483 (MapedFile.flush catch Throwable and advance):
 *
 * In MapedFile.java:
 * <pre>
 *   try {
 *       if (writeBuffer != null || this.fileChannel.position() != 0) {
 *           this.fileChannel.force(false);
 *       } else {
 *           this.mappedByteBuffer.force();
 *       }
 *   } catch (Throwable e) {
 *       log.error("Error occurred when force data to disk.", e);
 *   }
 *   this.flushedPosition.set(value);
 * </pre>
 *
 * When an I/O error or failure occurs during force(), the exception is caught, logged,
 * and flushedPosition is advanced as if the data had successfully reached non-volatile disk.
 *
 * Subsequent calls to isAbleToFlush() will think the data is already flushed and will NOT retry,
 * leading to silent loss of durability and data corruption upon crash.
 */
public class MapedFileFlushFailureSafetyTest {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testFlushMustNotAdvanceFlushedPositionWhenForceFails() throws Exception {
		File file = tempFolder.newFile("00000000000000000000");
		int fileSize = 4096;

		MapedFile mapedFile = new MapedFile(file.getAbsolutePath(), fileSize, true);

		// Append some data
		byte[] testData = "payload-requiring-durable-fsync".getBytes();
		boolean appendOk = mapedFile.appendData(testData);
		Assert.assertTrue("Append must succeed", appendOk);
		Assert.assertTrue("wrotePosition must be > 0", mapedFile.getWrotePostion() > 0);
		Assert.assertEquals("Initially flushedPosition must be 0", 0, mapedFile.getFlushedPosition());

		// Close the underlying fileChannel to force an IOException/ClosedChannelException during force()
		java.lang.reflect.Field channelField = MapedFile.class.getDeclaredField("fileChannel");
		channelField.setAccessible(true);
		FileChannel channel = (FileChannel) channelField.get(mapedFile);
		channel.close();

		// Flush least pages = 0 to trigger flush of uncommitted data
		mapedFile.flush(0);

		// SAFETY INVARIANT: If force() fails (fileChannel closed / I/O error), flushedPosition
		// MUST NOT advance to wrotePosition, because the data is NOT guaranteed to be on disk.
		// Advancing flushedPosition falsely signals to the caller and system that data is durable.
		Assert.assertEquals(
				"SAFETY DEFECT #2 CONFIRMED: flushedPosition must not advance when force() fails with an exception!",
				0,
				mapedFile.getFlushedPosition()
		);
	}
}
