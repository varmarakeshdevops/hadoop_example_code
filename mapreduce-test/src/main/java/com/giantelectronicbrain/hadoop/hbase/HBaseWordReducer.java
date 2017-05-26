/**
 * 
 */
package com.giantelectronicbrain.hadoop.hbase;

import com.giantelectronicbrain.hadoop.WordReducerBase;

/**
 * Reducer which does the summarizing word count step and puts the output into an HBase table.
 * 
 * @author tharter
 *
 */
public class HBaseWordReducer extends WordReducerBase {
	
	/**
	 * Construct an HBase based WordReducer.
	 */
	public HBaseWordReducer() {
		super();
		wordRepository = context.getBean(HbaseWordRepository.class);
	}

}
