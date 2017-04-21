/**
 * 
 */
package com.giantelectronicbrain.hadoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Accepts text keys and int values, sums all values for each given key, and outputs
 * key, sum.
 * 
 * @author tharter
 *
 */
public class HdfsFileReducer  extends Reducer<Text, IntWritable, Text, IntWritable> {

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		Iterator<IntWritable> vItr = values.iterator();
		while(vItr.hasNext()) {
			sum += vItr.next().get();
		}
		context.write(key, new IntWritable(sum));
	}

}
