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
package com.wuba.wpaxos.sample.maelstrom;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;

/**
 * Entry point for the Maelstrom lin-kv node.
 *
 * <p>This class exists solely to install the logging configuration before anything
 * touches log4j2, and it must therefore contain <em>no logger of its own</em>.
 * That constraint is not stylistic. {@link LinKvNode} declares a static
 * {@code Logger} field, and static fields are initialised when the class is
 * initialised -- which happens before {@code main} on that class can run. A
 * log4j2 logger created at that moment freezes the configuration, and log4j2's
 * built-in default appender writes to <b>stdout</b>. Since Maelstrom parses every
 * stdout line as a JSON protocol message, the result is a node whose first log
 * line aborts the entire test with a JSON parse error. Setting the configuration
 * from inside {@code LinKvNode.main} is already too late.
 *
 * <p>So the ordering is inverted: this class sets the property, and only then loads
 * {@link LinKvNode}, forcing its initialisation at a moment when log4j2 can still
 * be configured. {@code log4j2-maelstrom.xml} then routes everything to stderr,
 * which Maelstrom captures to the node's log file where it belongs.
 */
public final class MaelstromLinKV {

	private MaelstromLinKV() {
	}

	public static void main(String[] args) throws Exception {
		System.setProperty("log4j2.configurationFile", "log4j2-maelstrom.xml");

		// Hold a private handle on the real stdout. Everything the protocol loop writes
		// goes through this, and nothing else in the process is allowed near it.
		PrintStream out = new PrintStream(new FileOutputStream(FileDescriptor.out), true, "UTF-8");

		LinKvNode node = new LinKvNode(out);
		node.run();
	}
}
