/**
 * 
 */
package com.giantelectronicbrain.hadoop.cascading.tez;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import cascading.flow.FlowDef;

/**
 * Test building a local Cascading flow and executing it.
 * 
 * @author tharter
 *
 */
public class TezFlowTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();
	
	/**
	 * Build a local flow, execute it with some input, and compare the resulting output to
	 * expected output.
	 * 
	 * @throws IOException
	 */
	@Test
	public void test() throws IOException {
		final File actual = folder.newFile("tezexpectedoutput.txt");
		
		TezFlow tf = new TezFlow(TezFlow.LOCALINPATH,actual.getAbsolutePath(),true);
		FlowDef fd = tf.createFlowDef();
		tf.runFlow(fd);
		File expected = new File("./src/test/resources/tezexpectedoutput.txt");
		assertEquals(FileUtils.readLines(expected),FileUtils.readLines(actual));
	}

}
