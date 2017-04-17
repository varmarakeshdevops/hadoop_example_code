/**
 * 
 */
package com.giantelectronicbrain.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Mapper, simply reads text, splits it on words, and outputs each word as a key with a value of 1.
 * 
 * @author tharter
 *
 */
public class HdfsFileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String input = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(input);
		while(tokenizer.hasMoreTokens()) {
			String nextWord = tokenizer.nextToken();
			nextWord = nextWord.toLowerCase().trim().replaceAll("[.,\\-]", "");
			word.set(nextWord);
			context.write(word, one);
		}
	}

}
