/**
 * 
 */
package com.giantelectronicbrain.hadoop;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 
 * Simple DTO for interacting with various databases. Note that
 * this is immutable.
 * 
 * @author tharter
 *
 */
@Entity
@Table(name="words")
public class Word implements Comparable<Word> {
	private int count;
	@Id
	private String word;
	
	/**
	 * This is really only provided to make JPA happy.
	 */
	public Word() {
		
	}
	
	/**
	 * Create an initialized Word object. Note that these are immutable.
	 * 
	 * @param count the count, as a string.
	 * @param word the word.
	 */
	public Word(int count, String word) {
		this.count = count;
		this.word = word;
	}

	public int getCount() {
		return count;
	}

	public String getWord() {
		return word;
	}

	@Override
	public String toString() {
		return "Word [count=" + count + ", word=" + word + "]";
	}

	@Override
	public int compareTo(Word o) {
		return this.word.compareTo(o.word);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + count;
		result = prime * result + ((word == null) ? 0 : word.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Word other = (Word) obj;
		if (count != other.count)
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		return true;
	}

}
