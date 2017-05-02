package com.giantelectronicbrain.hadoop.spark;

import com.boeing.process.SparkContextUtil;

/**
 * 
 * 
 * @author MS Latha
 *
 */
public class WordCountApp {

	

	
	/**
	 * Initializes  example Spark  job. 
	 * This start up method takes an input file as an argument and create spark context to kick word count process. 
	 *  
	 * @param args
	 */
	
	
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\Users\\winutils");

		if (args.length == 0) {
			System.out.println("Usage: WordCount <file>");
			System.exit(0);
		}

		SparkContextUtil.wordCount(args[0]);
	}
}
