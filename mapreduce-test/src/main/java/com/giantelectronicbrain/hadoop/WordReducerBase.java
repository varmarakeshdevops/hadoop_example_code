/**
 * 
 */
package com.giantelectronicbrain.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.giantelectronicbrain.hadoop.springrepo.Driver;
import com.giantelectronicbrain.hadoop.springrepo.WordRepository;

/**
 * Reducer which does the summarizing word count step and puts the output into a repository.
 * 
 * @author tharter
 *
 */
public abstract class WordReducerBase extends Reducer<Text, IntWritable, Text, IntWritable> {
	protected static final Log LOG = LogFactory.getLog(WordReducerBase.class);
	protected AbstractApplicationContext context;
	protected IWordRepository wordRepository;
	
	protected WordReducerBase() {
//		setContext();
	}
	
//	protected void setContext() {
//		this.context = new ClassPathXmlApplicationContext("/META-INF/spring/application-context.xml",Driver.class);
//	}

	/**
	 * Save the given key/value into using our WordRepository.
	 * 
	 * @param word
	 * @param sum
	 * @throws RepositoryException 
	 */
	private void output(String word, int sum) throws RepositoryException {
		LOG.trace("Outputting word to repo "+word+"="+sum);
		wordRepository.save(word,sum);
	}
	
	/**
	 * Perform the basic reduce step logic. Called by hadoop.
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		Iterator<IntWritable> vItr = values.iterator();
		while(vItr.hasNext()) {
			sum += vItr.next().get();
		}
		try {
			output(key.toString(),sum);
		} catch (RepositoryException e) {
			LOG.error("failed to write to repository",e);
		}		
	}
}
