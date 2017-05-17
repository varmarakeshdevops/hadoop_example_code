package com.giantelectronicbrain.hadoop.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.giantelectronicbrain.hadoop.dao.WordDAO;
import com.giantelectronicbrain.hadoop.model.Word;

import scala.Tuple2;

/**
 * Created by Latha 
 * 	
 */
@Component
public class WordCount {

	private static final Log LOG = LogFactory.getLog(WordCount.class);
    
    private SparkConfig sparkConfig;
    
    private WordDAO wordDAO;



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
        
        //delete existing directory
        deleteExistingDir();
        
        counts.saveAsTextFile(outputDir);
        
        //Store all word count details in mongoDB
        List<Tuple2<String, Integer>> list = counts.collect();
        list.forEach(item->{
        	//insert each word & count in Word collection (MongoDB)
        	wordDAO.create(new Word(item._1, item._2));
        	
        });
        
    }
    
    public SparkConfig getSparkConfig() {
		return sparkConfig;
	}


	public void setSparkConfig(SparkConfig sparkConfig) {
		this.sparkConfig = sparkConfig;
	}
	
    public WordDAO getWordDAO() {
		return wordDAO;
	}

	public void setWordDAO(WordDAO wordDAO) {
		this.wordDAO = wordDAO;
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
	
	private void deleteExistingDir(){
		
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			// true stands for recursively deleting the folder you gave
			fs.delete(new Path(outputDir), true);
		} catch (Exception e) {
			LOG.error("Exception while deleting existing directory" +e.getMessage());
		}
		
	}
}
