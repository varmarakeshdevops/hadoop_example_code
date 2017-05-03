package com.giantelectronicbrain.hadoop.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import scala.Tuple2;

/**
 * Created by Latha 
 * 	
 */
@Component
public class WordCount {

	private static final Log LOG = LogFactory.getLog(WordCount.class);
    
    private SparkConfig sparkConfig;

    @Value("${spark.input.file}")
    private String inputFile;
    
    @Value("${spark.output.dir}")
    private String outputDir;

    @Value("${spark.input.threshold}")
    private int threshold;


    public void count() {
    	
    	JavaSparkContext javaSparkContext = sparkConfig.javaSparkContext();

        JavaRDD<String> tokenized = javaSparkContext.textFile(inputFile).flatMap((s1) -> Arrays.asList(s1.split(" ")));

        // count the occurrence of each word
        JavaPairRDD<String, Integer> counts = tokenized
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);
        
        counts.saveAsTextFile(outputDir);

        // filter out words with less than threshold occurrences
        JavaPairRDD<String, Integer> filtered = counts.filter(tup -> tup._2() >= threshold);

        // count characters
        JavaPairRDD<Character, Integer> charCounts = filtered.flatMap(
                s -> {
                    Collection<Character> chars = new ArrayList<>(s._1().length());
                    for (char c : s._1().toCharArray()) {
                        chars.add(c);
                    }
                    return chars;
                }
        ).mapToPair(c -> new Tuple2<>(c, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        LOG.info(charCounts.collect());
    }
    
    public SparkConfig getSparkConfig() {
		return sparkConfig;
	}


	public void setSparkConfig(SparkConfig sparkConfig) {
		this.sparkConfig = sparkConfig;
	}


	public String getInputFile() {
		return inputFile;
	}


	public void setInputFile(String inputFile) {
		this.inputFile = inputFile;
	}


	public String getOutputDir() {
		return outputDir;
	}


	public void setOutputDir(String outputDir) {
		this.outputDir = outputDir;
	}


	public int getThreshold() {
		return threshold;
	}


	public void setThreshold(int threshold) {
		this.threshold = threshold;
	}
}
