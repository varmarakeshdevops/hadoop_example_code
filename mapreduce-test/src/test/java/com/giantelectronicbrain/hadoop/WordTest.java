/**
 * 
 */
package com.giantelectronicbrain.hadoop;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Basic tests of DTO.
 * 
 * @author tharter
 *
 */
public class WordTest {

	/**
	 * Test method for {@link com.giantelectronicbrain.hadoop.Word#getCount()}.
	 */
	@Test
	public void testGetCount() {
		Word word = new Word(123,"testing");
		int cv = word.getCount();
		assertEquals("Count should match",123,cv);
	}

	/**
	 * Test method for {@link com.giantelectronicbrain.hadoop.Word#getWord()}.
	 */
	@Test
	public void testGetWord() {
		Word word = new Word(123,"testing");
		String wv = word.getWord();
		assertEquals("Word should match","testing",wv);
	}

	/**
	 * Test method for {@link com.giantelectronicbrain.hadoop.Word#compareTo(com.giantelectronicbrain.hadoop.Word)}.
	 */
	@Test
	public void testCompareTo() {
		Word w1 = new Word(123,"testing");
		Word w2 = new Word(456,"testing2");
		int r = w1.compareTo(w2);
		assertEquals("w1 should be less than w2",-1,r);
		w1 = new Word(123,"testing3");
		r = w1.compareTo(w2);
		assertEquals("w1 should be greater than w2",1,r);
		w1 = new Word(123,"testing2");
		r = w1.compareTo(w2);
		assertEquals("w1 should be equal to w2",0,r);
	}

	/**
	 * Test method for {@link java.lang.Object#hashCode()}.
	 */
	@Test
	public void testHashCode() {
		Word w1 = new Word(123,"testing");
		Word w2 = new Word(456,"testing2");
		assertTrue("hashes should be unequal", w1.hashCode() != w2.hashCode());
		w1 = new Word(123,"testing2");
		assertTrue("hashes should be unequal", w1.hashCode() != w2.hashCode());
		w1 = new Word(456,"testing2");
		assertTrue("hashes should be equal", w1.hashCode() == w2.hashCode());
	}

	/**
	 * Test method for {@link java.lang.Object#equals(java.lang.Object)}.
	 */
	@Test
	public void testEquals() {
		Word w1 = new Word(123,"testing");
		Word w2 = new Word(456,"testing2");
		assertTrue("words should not be equal",!w1.equals(w2));
		w1 = new Word(123,"testing2");
		assertTrue("words should not be equal",!w1.equals(w2));
		w1 = new Word(456,"testing2");
		assertTrue("words should be equal",w1.equals(w2));
		assertTrue("equality should be transitive",w2.equals(w1));
	}

}
