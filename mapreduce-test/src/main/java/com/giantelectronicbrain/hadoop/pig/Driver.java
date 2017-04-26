/**
 * 
 */
package com.giantelectronicbrain.hadoop.pig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.pig.PigRunner;

/**
 * Uses Spring PigRunner to run a pig script.
 * 
 * @author tharter
 *
 */
public class Driver {
	private static final Log LOG = LogFactory.getLog(Driver.class);

	/**
	 * Drive example PIG script. 
	 * 
	 * @param args these are unused.
	 * @throws Exception if something goes wrong
	 */
	public static void main(String[] args) throws Exception {
		@SuppressWarnings("resource")
		AbstractApplicationContext context = new ClassPathXmlApplicationContext("/META-INF/spring/application-context.xml",Driver.class);
		LOG.info("Pig script Application Running");
		context.registerShutdownHook();
		
		try {
			LOG.info("Going to try to run a PIG script defined by Spring");
			PigRunner pigRunner = (PigRunner) context.getBean("pigRunner");
			pigRunner.call();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
	}

}
