/**
 * 
 */
package com.giantelectronicbrain.hadoop.hive;

/**
 * @author tharter
 *
 */
//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(locations = { "classpath:test-context.xml" })
public class WordRepositoryTest {

// Note: none of this is really working yet, as I can't seem to get hive to run an insert correctly.
	
//	@Autowired
//	WordRepository uut;
//
//	@Before
//	public void setup() throws TException {
//		uut.initTable(); // make sure table exists
//		uut.clearTable(); // insure known table state
//	}
//	
//	/*
//	 * Test method for {@link com.giantelectronicbrain.hadoop.hbase.WordRepository#clearTable()}.
//	 */
//	@Test
//	public void testClearTable() {
//		uut.save("666", "waldo");
//		uut.clearTable();
//		List<Word> words = uut.findAll();
//		assertNotNull(words);
//		assertEquals("should be zero words",0,words.size());
//	}
//	
//	/**
//	 * Test method for {@link com.giantelectronicbrain.hadoop.hbase.WordRepository#findAll()}.
//	 */
//	@Test
//	public void testFindAll() {
//		uut.save("666", "waldo");
//		List<Word> words = uut.findAll();
//		assertNotNull(words);
//		assertEquals("should be 1 words",1,words.size());
//		Word word = words.get(0);
//		assertEquals("word should be 'waldo'","waldo",word.getWord());
//		assertEquals("Count should be '666'","666",word.getCount());
//	}
//
//	/**
//	 * Test method for {@link com.giantelectronicbrain.hadoop.hbase.WordRepository#initTable()}.
//	 */
//	@Test
//	public void testDeleteTable() {
//		try {
//			uut.deleteTable();
//			boolean tExists = uut.tableExists();
//			assertTrue("table should not exist",!tExists);
//		} catch (IOException e) {
//			fail("Delete table threw exception");
//			e.printStackTrace();
//		}
//	}
//
//	/**
//	 * Test method for {@link com.giantelectronicbrain.hadoop.hbase.WordRepository#initTable()}.
//	 * @throws IOException 
//	 */
//	@Test
//	public void testInitTable() throws IOException {
//		uut.deleteTable();
//		uut.initTable();
//		assertTrue("table should exist",uut.tableExists());
//	}

}
