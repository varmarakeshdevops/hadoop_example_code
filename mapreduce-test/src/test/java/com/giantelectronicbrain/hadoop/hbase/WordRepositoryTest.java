/**
 * 
 */
package com.giantelectronicbrain.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.giantelectronicbrain.hadoop.Word;

/**
 * @author tharter
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:test-context.xml" })
public class WordRepositoryTest {

	@Autowired
	WordRepository uut;

	@Before
	public void setup() throws IOException {
		uut.initTable(); // make sure table exists
		uut.clearTable(); // insure known table state
	}
	
	/*
	 * Test method for {@link com.giantelectronicbrain.hadoop.hbase.WordRepository#clearTable()}.
	 */
	@Test
	public void testClearTable() {
		uut.save("666", "waldo");
		uut.clearTable();
		List<Word> words = uut.findAll();
		assertNotNull(words);
		assertEquals("should be zero words",0,words.size());
	}
	
	/**
	 * Test method for {@link com.giantelectronicbrain.hadoop.hbase.WordRepository#findAll()}.
	 */
	@Test
	public void testFindAll() {
		uut.save("666", "waldo");
		List<Word> words = uut.findAll();
		assertNotNull(words);
		assertEquals("should be 1 words",1,words.size());
		Word word = words.get(0);
		assertEquals("word should be 'waldo'","waldo",word.getWord());
		assertEquals("Count should be '666'","666",word.getCount());
	}

	/**
	 * Test method for {@link com.giantelectronicbrain.hadoop.hbase.WordRepository#initTable()}.
	 */
	@Test
	public void testDeleteTable() {
		try {
			uut.deleteTable();
			boolean tExists = uut.tableExists();
			assertTrue("table should not exist",!tExists);
		} catch (IOException e) {
			fail("Delete table threw exception");
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link com.giantelectronicbrain.hadoop.hbase.WordRepository#initTable()}.
	 * @throws IOException 
	 */
	@Test
	public void testInitTable() throws IOException {
		uut.deleteTable();
		uut.initTable();
		assertTrue("table should exist",uut.tableExists());
	}

}
