/**
 * 
 */
package com.giantelectronicbrain.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Test;

/**
 * Test the Mapper and Reducer together from MapReducePractice.
 * 
 * @author tharter
 *
 */
public class HdfsMapperReducerTest {
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	HdfsFileMapper mapper = new HdfsFileMapper();
	HdfsFileReducer reducer = new HdfsFileReducer();
	
	@Test
	public void test() throws IOException {
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
		
		mapReduceDriver.setMapInputPath(new Path("test1"));
		mapReduceDriver.addInput(new LongWritable(), new Text("firstword"));
		mapReduceDriver.setMapInputPath(new Path("test2"));
		mapReduceDriver.addInput(new LongWritable(), new Text("firstword"));
		mapReduceDriver.addOutput(new Text("firstword"),new IntWritable(2));
		
		mapReduceDriver.run();
	}

}
