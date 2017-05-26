package com.giantelectronicbrain.hadoop.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;

import com.giantelectronicbrain.hadoop.IWordRepository;
import com.giantelectronicbrain.hadoop.RepositoryException;
import com.giantelectronicbrain.hadoop.Word;

/**
 * Spring Repository stereotype to push words and counts into an HBase Table.
 * 
 * @author tharter
 *
 */
@SuppressWarnings("deprecation")
@org.springframework.stereotype.Repository
public class HbaseWordRepository implements IWordRepository {

	private volatile HBaseAdmin hbaseAdmin = null;
	
	@Autowired
	private HbaseTemplate hbaseTemplate;

	private String tableName = "words";

	/**
	 * Get the name of the HBase table being used.
	 * 
	 * @return String table name.
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * Set the name of the HBase table being used.
	 * 
	 * @param tableName String table name.
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	private static byte[] CF_INFO = Bytes.toBytes("cfInfo");
	private static byte[] qCount = Bytes.toBytes("count");
	private static byte[] qWord = Bytes.toBytes("word");

	/**
	 * Return a dump of the whole table. This obviously will NOT scale! It is just useful for tests.
	 * 
	 * @return List&lt;Word&gt; all the words in the table.
	 */
	public List<Word> findAll() {
		return hbaseTemplate.find(tableName, "cfInfo", new RowMapper<Word>() {
			@Override
			public Word mapRow(Result result, int rowNum) throws Exception {
				return new Word(Bytes.toInt(result.getValue(CF_INFO, qCount)), 
						Bytes.toString(result.getValue(CF_INFO, qWord)));
			}
		});

	}
	
	/*
	 * Use this to access an HBaseAdmin object, as it provides caching.
	 */
	private synchronized HBaseAdmin getHBaseAdmin() throws IOException {
		if(hbaseAdmin == null) {
			hbaseAdmin = new HBaseAdmin(hbaseTemplate.getConfiguration());
		}
		return hbaseAdmin;
	}
	
	/**
	 * Returns true if the words table exists.
	 * 
	 * @return boolean true if table exists
	 * @throws RepositoryException if something goes wrong with the query.
	 */
	public boolean tableExists() throws RepositoryException {
		try {
			return getHBaseAdmin().tableExists(tableName);
		} catch (IOException e) {
			throw new RepositoryException(e.getMessage(),e);
		}
	}

	/**
	 * Delete the words table if it exists.
	 * 
	 * @throws RepositoryException if the table cannot be deleted.
	 */
	public void deleteTable() throws RepositoryException {
		try {
			HBaseAdmin admin = getHBaseAdmin();
			if(admin.tableExists(tableName)) {
				if(!admin.isTableDisabled(tableName)) {
					admin.disableTable(tableName);
				}
				admin.deleteTable(tableName);
			}
		} catch (IOException e) {
			throw new RepositoryException(e.getMessage(),e);
		}
	}

	/**
	 * Remove all rows from the words table. Note that this probably won't scale nicely.
	 * 
	 */
	public void clearTable() {
		List<Word> words = findAll();
		for(Word word : words) {
			deleteRow(word.getWord());
		}
	}
	
	/**
	 * Delete a row with the given word as key.
	 * 
	 * @param key word to delete
	 */
	public void deleteRow(String key) {
		hbaseTemplate.delete(tableName, key, "cfInfo");
	}
	
	/**
	 * Create the words table if it doesn't exist in the default namespace. 
	 * 
	 * @throws RepositoryException if the table cannot be initialized.
	 */
	public void initTable() throws RepositoryException {
		try {
			HBaseAdmin admin = getHBaseAdmin();
			if(!admin.tableExists(tableName)) {
				HTableDescriptor descriptor = new HTableDescriptor(Bytes.toBytes(tableName));
				HColumnDescriptor column = new HColumnDescriptor(CF_INFO);
				descriptor.addFamily(column);
				admin.createTable(descriptor);
			}
			// Some versions of HBase seem to require this, other's don't.
			if(admin.isTableDisabled(tableName))
				admin.enableTable(tableName);
		} catch (IOException e) {
			throw new RepositoryException(e.getMessage(),e);
		}
	}

	/**
	 * Save a Word to the repository.
	 * 
	 * @param wordDTO Word to save
	 * @return saved Word
	 */
	public Word save(final Word wordDTO) {
		return hbaseTemplate.execute(tableName, new TableCallback<Word>() {
			public Word doInTable(HTableInterface table) throws Throwable {
				Put p = new Put(Bytes.toBytes(wordDTO.getWord()));
				p.addColumn(CF_INFO, qCount, Bytes.toBytes(wordDTO.getCount()));
				p.addColumn(CF_INFO, qWord, Bytes.toBytes(wordDTO.getWord()));
				table.put(p);
				return wordDTO;
				
			}
		});
	}
	
	/**
	 * Save a word count to the HBase table. Word is the key.
	 * 
	 * @param word the word
	 * @param count how often the word appears
	 * @return a Word object
	 */
	public Word save(final String word, final int count) {
		return hbaseTemplate.execute(tableName, new TableCallback<Word>() {
			public Word doInTable(HTableInterface table) throws Throwable {
				Word wordDTO = new Word(count, word);
				String key = wordDTO.getWord();
				if(key != null && key.length() > 0) {
					Put p = new Put(Bytes.toBytes(wordDTO.getWord()));
					p.addColumn(CF_INFO, qCount, Bytes.toBytes(wordDTO.getCount()));
					p.addColumn(CF_INFO, qWord, Bytes.toBytes(wordDTO.getWord()));
					table.put(p);
				}
				return wordDTO;
				
			}
		});
	}

	/**
	 * This doesn't really do anything in this implementation.
	 */
	@Override
	public void close() {
	}

}