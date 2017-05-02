/**
 * 
 */
package com.giantelectronicbrain.hadoop.springrepo;

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
 * Basic talking to databases via a JPA version of WordRepository.
 * 
 * @author tharter
 *
 */
public class Driver {
	private static final Log LOG = LogFactory.getLog(Driver.class);

	/**
	 * Perform some basic operations using a WordRepository
	 * 
	 * @throws Exception if anything goes wrong.
	 */
	public static void testClient() throws Exception {
		@SuppressWarnings("resource")
		AbstractApplicationContext context = new ClassPathXmlApplicationContext("/META-INF/spring/application-context.xml",Driver.class);
		LOG.info("MapReduce Example with HDFS copy and JPA import Application Running");
		context.registerShutdownHook();
		
		LOG.info("Creating a database table");
		IWordRepository wordRepository = context.getBean(WordRepository.class);
		wordRepository.initTable();
		wordRepository.clearTable();
		
		LOG.info("Performing map of HDFS data and push results to JPA");
		JobRunner jpaRunner = (JobRunner) context.getBean("jpaRunner");
		jpaRunner.call();
		
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
	 * Runs a MapReduce job which sinks data to a JPA data source.
	 * 
	 * @param args command line arguments.
	 */
	public static void main(String[] args) {
		try {
			testClient();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("what is the cause"+e.getCause());
		}
	}

}
