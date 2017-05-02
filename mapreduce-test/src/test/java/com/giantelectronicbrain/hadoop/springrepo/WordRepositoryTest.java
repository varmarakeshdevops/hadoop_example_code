/**
 * 
 */
package com.giantelectronicbrain.hadoop.springrepo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.giantelectronicbrain.hadoop.IWordRepository;
import com.giantelectronicbrain.hadoop.RepositoryException;
import com.giantelectronicbrain.hadoop.Word;

/**
 * 
 * Unit test the JPA implementation of IWordRepository.
 * 
 * @author tharter
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:jpatest-context.xml" })
public class WordRepositoryTest {

	@Autowired
	IWordRepository uut;

	/**
	 * Put the UUT in a known state before each test.
	 * 
	 * @throws RepositoryException
	 */
	@Before
	public void setup() throws RepositoryException {
		uut.initTable(); // make sure table exists
		uut.clearTable(); // insure known table state
	}
	
	/**
	 * Test method for {@link com.giantelectronicbrain.hadoop.springrepo.WordRepository#clearTable()}.
	 * @throws RepositoryException 
	 */
	@Test
	public void testClearTable() throws RepositoryException {
		uut.save("waldo",666);
		uut.clearTable();
		List<Word> words = uut.findAll();
		assertNotNull(words);
		assertEquals("should be zero words",0,words.size());
	}
	
	/**
	 * Test method for {@link com.giantelectronicbrain.hadoop.springrepo.WordRepository#findAll()}.
	 * @throws RepositoryException 
	 */
	@Test
	public void testFindAll() throws RepositoryException {
		uut.save("waldo",666);
		List<Word> words = uut.findAll();
		assertNotNull(words);
		assertEquals("should be 1 words",1,words.size());
		Word word = words.get(0);
		assertEquals("word should be 'waldo'","waldo",word.getWord());
		assertEquals("Count should be '666'",666,word.getCount());
	}

}
