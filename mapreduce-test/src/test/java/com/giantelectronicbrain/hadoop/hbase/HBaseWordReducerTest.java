/**
 * 
 */
package com.giantelectronicbrain.hadoop.hbase;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.giantelectronicbrain.hadoop.hbase.HBaseWordReducer;

/**
 * Test the Reducer alone from MapReducePractice.
 * 
 * @author tharter
 *
 */
public class HBaseWordReducerTest {
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	HBaseWordReducer reducer;
	
	@Before
	public void init() {
		try {
			reducer = new HBaseWordReducer();
		} catch (Throwable t) {
			t.printStackTrace();
			fail("Creating mod reducer failed: "+t.getLocalizedMessage());
		}
	}
	
	@Test
	public void testMapper() throws IOException {
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		
		List<IntWritable> values = new ArrayList<IntWritable>();
		IntWritable int1 = new IntWritable(1);
		IntWritable int2 = new IntWritable(2);
		values.add(int1);
		values.add(int2);
		
		reduceDriver.withInput(new Text("foofoo"),values);
//		reduceDriver.withOutput(new Text("foofoo"),new IntWritable(3));
		reduceDriver.runTest();
	}

}
