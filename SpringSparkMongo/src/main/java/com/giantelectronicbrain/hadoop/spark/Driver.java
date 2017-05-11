package com.giantelectronicbrain.hadoop.spark;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


public class Driver {
	
	private static final Log LOG = LogFactory.getLog(Driver.class);
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\Users\\winutils");
		@SuppressWarnings("resource")
		AbstractApplicationContext context = new ClassPathXmlApplicationContext("/spark-context.xml",Driver.class);
		LOG.info("Spring Spark Example Starting ");
		context.registerShutdownHook();
		
		WordCount wordCount = (WordCount) context.getBean("wordCount");
		wordCount.count();
		LOG.info("Spring Spark Word Count Example End ");
 
	}

}
