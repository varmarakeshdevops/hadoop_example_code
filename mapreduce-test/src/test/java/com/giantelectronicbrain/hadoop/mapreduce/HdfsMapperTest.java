/**
 * 
 */
package com.giantelectronicbrain.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test the Mapper alone from MapReducePractice.
 * 
 * @author tharter
 *
 */
public class HdfsMapperTest {
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	HdfsFileMapper mapper = new HdfsFileMapper();
	
	@Test
	public void testMapper() throws IOException {
		mapDriver = MapDriver.newMapDriver(mapper);
		Mapper<LongWritable, Text, Text, IntWritable>.Context context = mapDriver.getContext();
		
		InputSplit inputSplit = Mockito.mock(InputSplit.class);
		Mockito.when(context.getInputSplit()).thenReturn(inputSplit);
		Mockito.when(inputSplit.toString()).thenReturn("test1");
		
		Text expected = new Text("nebelwerfer");
		mapDriver.withInput(new LongWritable(), expected);
		IntWritable intWritable = new IntWritable();
		intWritable.set(1);
		
		mapDriver.withOutput(expected,intWritable);
		mapDriver.runTest();
		
		mapDriver = MapDriver.newMapDriver(mapper);
		Text expected2 = new Text("nashorn");
		Mockito.when(inputSplit.toString()).thenReturn("test2");
		mapDriver.withInput(new LongWritable(), expected2);
		intWritable = new IntWritable();
		intWritable.set(1);
		mapDriver.withOutput(expected2,intWritable);
		mapDriver.runTest();
	}

}
