/**
 * 
 */
package com.giantelectronicbrain.hadoop.mapreduce;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.mapreduce.JobRunner;
import org.springframework.data.hadoop.pig.PigRunner;

import com.giantelectronicbrain.hadoop.Word;
import com.giantelectronicbrain.hadoop.hbase.WordRepository;

/**
 * Just do some practicing with building a MapReduce setup and running it. This
 * uses MR2 APIs and Spring Hadoop for setup. 
 * 
 * @author tharter
 *
 */
public class Driver {
	private static final Log LOG = LogFactory.getLog(Driver.class);

	/**
	 * Drive example MapReduce jobs. 
	 * 
	 * Loads 2 jobs out of Spring Application Context and executes them. Also
	 * loads a Spring Repository and initializes an HBase table with it. Each
	 * job also runs a Groovy script to pull data into HDFS and delete stale target
	 * files.
	 * Finally, a PIG job will be executed which does basically the same thing. The
	 * actual Pig script is defined in the application-context.xml.
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		@SuppressWarnings("resource")
		AbstractApplicationContext context = new ClassPathXmlApplicationContext("/META-INF/spring/application-context.xml",Driver.class);
		LOG.info("MapReduce Example with HDFS copy and HBase import Application Running");
		context.registerShutdownHook();
		
		LOG.info("Performing basic HDFS file processing, outputs results to HDFS");
		JobRunner mrRunner = (JobRunner) context.getBean("runner");
		mrRunner.call();
		
		LOG.info("Creating an HBase table");
	    WordRepository wordRepository = context.getBean(WordRepository.class);
	    wordRepository.initTable();
	    wordRepository.clearTable();
	    
	    LOG.info("Performing map of HDFS data and push results to HBase");
		JobRunner hbaseRunner = (JobRunner) context.getBean("hbaseRunner");
		hbaseRunner.call();
		
		List<Word> words = wordRepository.findAll();
		words.sort(null);
		System.out.println("Number of Words = "+words.size());
		for(Word word : words) {
			System.out.println(word);
		}

		LOG.info("Going to try to run a PIG script defined by Spring");
		PigRunner pigRunner = (PigRunner) context.getBean("pigRunner");
		pigRunner.call();
	}

}
