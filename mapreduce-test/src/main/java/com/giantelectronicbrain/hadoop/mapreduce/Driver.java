/**
 * 
 */
package com.giantelectronicbrain.hadoop.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.mapreduce.JobRunner;

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
	 * Drive example MapReduce job. 
	 * 
	 * Loads a jobs out of Spring Application Context and executes it. The
	 * job also runs a Groovy script to pull data into HDFS and delete stale target
	 * files.
	 * 
	 * @param args these are unused.
	 * @throws Exception if something goes wrong
	 */
	public static void main(String[] args) throws Exception {
		@SuppressWarnings("resource")
		AbstractApplicationContext context = new ClassPathXmlApplicationContext("/META-INF/spring/application-context.xml",Driver.class);
		LOG.info("MapReduce Example with HDFS copy");
		context.registerShutdownHook();
		
		try {
			LOG.info("Performing basic HDFS file processing, outputs results to HDFS");
			JobRunner mrRunner = (JobRunner) context.getBean("runner");
			mrRunner.call();
		} catch (Exception e2) {
			e2.printStackTrace();
		}
	}

}
