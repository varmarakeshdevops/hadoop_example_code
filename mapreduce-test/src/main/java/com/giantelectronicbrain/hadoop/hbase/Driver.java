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

/**
 * Driver which runs just the hbase mapping part of the task. This also dumps out a view of the resulting
 * HBase table using the Hive API.
 * 
 * @author tharter
 * 
 */
public class Driver {
	private static final Log LOG = LogFactory.getLog(Driver.class);
	private static AbstractApplicationContext context;

	/**
	 * Driver which sinks word counts into an HBase table.
	 * 
	 * @param args unused
	 */
	public static void main(String[] args) {
		try {
		    context = new ClassPathXmlApplicationContext("/META-INF/spring/application-context.xml",Driver.class);
		    
		    HbaseWordRepository wordRepository = context.getBean(HbaseWordRepository.class);
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
