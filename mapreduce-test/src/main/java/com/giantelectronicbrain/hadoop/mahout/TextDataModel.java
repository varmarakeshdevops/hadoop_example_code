/**
 * 
 */
package com.giantelectronicbrain.hadoop.mahout;

import java.util.Iterator;

import com.giantelectronicbrain.hadoop.Word;

/**
 * Holds word count distribution information about texts.
 * 
 * @author tharter
 *
 */
public class TextDataModel {
	
	public Iterator<Word> getWordIterator() {
		return new WordIterator(this);
	}
	
//	public 
}
