/**
 * 
 */
package com.giantelectronicbrain.hadoop;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.mapreduce.JobRunner;
import org.springframework.data.hadoop.pig.PigRunner;

import com.giantelectronicbrain.hadoop.Word;
import com.giantelectronicbrain.hadoop.cascading.tez.TezFlow;
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
	 * Loads all the POC jobs and executes them all.
	 * 
	 * @param args these are unused.
	 * @throws Exception if something goes wrong
	 */
	public static void main(String[] args) throws Exception {
		@SuppressWarnings("resource")
		AbstractApplicationContext context = new ClassPathXmlApplicationContext("/META-INF/spring/application-context.xml",Driver.class);
		LOG.info("MapReduce Example with HDFS copy and HBase import Application Running");
		context.registerShutdownHook();
		
		try {
			LOG.info("Performing basic HDFS file processing, outputs results to HDFS");
			JobRunner mrRunner = (JobRunner) context.getBean("runner");
			mrRunner.call();
		} catch (Exception e2) {
			e2.printStackTrace();
		}
		
	    try {
			LOG.info("Creating an HBase table");
		    IWordRepository wordRepository = context.getBean(WordRepository.class);
		    wordRepository.initTable();
		    wordRepository.clearTable();
		    
			LOG.info("Performing map of HDFS data and push results to HBase");
			JobRunner hbaseRunner = (JobRunner) context.getBean("hbaseRunner");
			hbaseRunner.call();
			
			dumpWordRepository(wordRepository);

		} catch (Exception e1) {
			e1.printStackTrace();
		}
	    
	    try {
			LOG.info("Creating a Hive table");
			IWordRepository wordRepository = (IWordRepository) context.getBean("hiveWordRepository");
			wordRepository.initTable();
			wordRepository.clearTable();
			
			LOG.info("Performing map of HDFS data and push results to Hive");
			JobRunner hiveRunner = (JobRunner) context.getBean("hiveRunner");
			hiveRunner.call();
			
			dumpWordRepository(wordRepository);

		} catch (Exception e) {
			e.printStackTrace();
		}
		
	    try {
			LOG.info("Creating a JPA table");
			IWordRepository wordRepository = context.getBean(com.giantelectronicbrain.hadoop.springrepo.WordRepository.class);
			wordRepository.initTable();
			wordRepository.clearTable();
			
			LOG.info("Performing map of HDFS data and push results to JPA");
			JobRunner jpaRunner = (JobRunner) context.getBean("jpaRunner");
			jpaRunner.call();
			
			dumpWordRepository(wordRepository);

		} catch (Exception e) {
			e.printStackTrace();
		}

	    try {
			LOG.info("Going to try to run a PIG script defined by Spring");
			PigRunner pigRunner = (PigRunner) context.getBean("pigRunner");
			pigRunner.call();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		try {
			LOG.info("Execute Cascading Tez job");
			TezFlow.DoTezFlow(false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void dumpWordRepository(IWordRepository wordRepository) throws RepositoryException {
		List<Word> words = wordRepository.findAll();
		words.sort(null);
		System.out.println("Number of Words = "+words.size());
		for(Word word : words) {
			System.out.println(word);
		}
	}
}
