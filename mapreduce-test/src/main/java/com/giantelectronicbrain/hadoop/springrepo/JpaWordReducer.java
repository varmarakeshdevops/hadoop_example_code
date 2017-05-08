/**
 * 
 */
package com.giantelectronicbrain.hadoop.springrepo;

import com.giantelectronicbrain.hadoop.WordReducerBase;

/**
 * Reducer which does the summarizing word count step and puts the output into a JPA repository.
 * 
 * @author tharter
 *
 */
public class JpaWordReducer extends WordReducerBase {
	
	/**
	 * Construct a JPA based WordReducer.
	 */
	public JpaWordReducer() {
		super();
		wordRepository = context.getBean(WordRepository.class);
	}
	

}
