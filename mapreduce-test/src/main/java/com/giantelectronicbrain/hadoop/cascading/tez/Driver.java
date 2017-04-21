/**
 * 
 */
package com.giantelectronicbrain.hadoop.cascading.tez;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

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
	 * @param args command line arguments, first argument is optionally local/remote flag.
	 * @throws Exception if something goes wrong
	 */
	public static void main(String[] args) throws Exception {
		boolean isLocal = args.length > 0 ? Boolean.valueOf(args[0]) : false;
		@SuppressWarnings("resource")
		AbstractApplicationContext context = new ClassPathXmlApplicationContext("/META-INF/spring/application-context.xml",Driver.class);
		LOG.info("MapReduce Example with HDFS copy and HBase import Application Running");
		context.registerShutdownHook();
		
		LOG.info("Execute Cascading Tez job");
//		CascadingRunner cRunner = (CascadingRunner) context.getBean("cascade");
//		cRunner.call();
		TezFlow.DoTezFlow(isLocal);
	}

}
