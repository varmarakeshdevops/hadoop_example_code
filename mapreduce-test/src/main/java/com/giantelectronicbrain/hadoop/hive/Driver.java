/**
 * 
 */
package com.giantelectronicbrain.hadoop.hive;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.mapreduce.JobRunner;

import com.giantelectronicbrain.hadoop.IWordRepository;
import com.giantelectronicbrain.hadoop.RepositoryException;
import com.giantelectronicbrain.hadoop.Word;

/**
 * Basic talking to Hive via a Hive version of WordRepository.
 * 
 * @author tharter
 *
 */
public class Driver {
	private static final Log LOG = LogFactory.getLog(Driver.class);

	private static final String DEFAULT_HIVE_SERVER_LOCATION = "localhost";
	private static final int DEFAULT_HIVE_SERVER_PORT = 10000;

	/**
	 * Perform some basic operations using a WordRepository
	 * 
	 * @throws Exception if anything goes wrong.
	 */
	public static void testThriftClient() throws Exception {
		@SuppressWarnings("resource")
		AbstractApplicationContext context = new ClassPathXmlApplicationContext("/META-INF/spring/application-context.xml",Driver.class);
		LOG.info("MapReduce Example with HDFS copy and HBase import Application Running");
		context.registerShutdownHook();
		
		LOG.info("Creating a Hive table");
		IWordRepository wordRepository = (IWordRepository) context.getBean("hiveWordRepository");
		wordRepository.initTable();
		wordRepository.clearTable();
		
		LOG.info("Performing map of HDFS data and push results to Hive");
		JobRunner hiveRunner = (JobRunner) context.getBean("hiveRunner");
		hiveRunner.call();
		
		dumpWordRepository(wordRepository);

    }

	private static void dumpWordRepository(IWordRepository wordRepository) throws RepositoryException {
		List<Word> words = wordRepository.findAll();
		words.sort(null);
		System.out.println("Number of Words = "+words.size());
		for(Word word : words) {
			System.out.println(word);
		}
	}
	
	/**
	 * Accepts a hostname and port, connects to the given Hive server via thrift, and 
	 * tries some basic operations. If no arguments are supplied, defaults will be used.
	 * 
	 * @param args command line arguments.
	 */
	public static void main(String[] args) {
		try {
			testThriftClient();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("what is the cause"+e.getCause());
		}
	}

}
