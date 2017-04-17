/**
 * 
 */
package com.giantelectronicbrain.hadoop;

/**
 * 
 * Simple DTO for interacting with various databases. Note that
 * this is immutable.
 * 
 * @author tharter
 *
 */
public class Word implements Comparable<Word> {
	private final String count;
	private final String word;
	
	public Word(String count, String word) {
		this.count = count;
		this.word = word;
	}

	public String getCount() {
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
		result = prime * result + ((count == null) ? 0 : count.hashCode());
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
		if (count == null) {
			if (other.count != null)
				return false;
		} else if (!count.equals(other.count))
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		return true;
	}

}
