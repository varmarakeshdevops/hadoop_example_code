/**
 * 
 */
package com.giantelectronicbrain.hadoop.hbase;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.mapreduce.JobRunner;

import com.giantelectronicbrain.hadoop.Word;
import com.giantelectronicbrain.hadoop.mapreduce.Driver;

/**
 * Driver which runs just the hbase mapping part of the task. This also dumps out a view of the resulting
 * HBase table using the Hive API.
 * 
 * @author tharter
 *
 */
public class HBaseOutput {
	private static final Log LOG = LogFactory.getLog(Driver.class);
	private static AbstractApplicationContext context;

	/**
	 * Interestingly you CAN use Spring within the Job Runner, because that is a whole standalone application that is
	 * launched once by Hadoop to manage actual submission of various jobs. It can also do 'stuff' itself, although it is
	 * not advisable to do any data processing here, since this is just one single task. In any case, you can inject stuff
	 * here etc.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
		    context = new ClassPathXmlApplicationContext("/META-INF/spring/application-context.xml",HBaseOutput.class);
		    
		    WordRepository wordRepository = context.getBean(WordRepository.class);
		    wordRepository.initTable();
		    
			JobRunner hbaseRunner = (JobRunner) context.getBean("hbaseRunner");
			hbaseRunner.call();
			LOG.info("HBaseOutput Application Running");
			context.registerShutdownHook();
			
			List<Word> words = wordRepository.findAll();
			System.out.println("Number of Words = "+words.size());
			System.out.println(words);
			
		} catch (Exception e) {
			LOG.error("HBaseOutput failed with Exception",e);
			e.printStackTrace();
		}
	}

}
