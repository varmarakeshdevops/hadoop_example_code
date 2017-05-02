package com.giantelectronicbrain.hadoop;

import java.util.List;

import org.apache.thrift.TException;

/**
 * Interface to generalize a WordRepository. This can be implemented to support whatever storage
 * scheme is desired.
 * 
 * @author tharter
 *
 */
public interface IWordRepository {

	/**
	 * Get the name of the table.
	 * 
	 * @return String table name
	 */
	String getTableName();

	/**
	 * Set the name of the table. Set this before you make queries.
	 * 
	 * @param tableName table name to use
	 */
	void setTableName(String tableName);

	/**
	 * Close the repository connection. This should be used to clean up resources.
	 */
	void close();

	/**
	 * Find all the words in the table and return them all as a List. <b>Note:</b> this really
	 * won't scale well.
	 * 
	 * @return List&lt;Word&gt; return records as a list in TSV form.
	 * 
	 * @throws RepositoryException if the query fails.
	 */
	List<Word> findAll() throws RepositoryException;

	/**
	 * Make sure that the table exists, create it if it doesn't.
	 * 
	 * @throws RepositoryException if the table cannot be created or there is some other Hive error.
	 */
	void initTable() throws RepositoryException;

	/**
	 * Save a word into the table.
	 * 
	 * @param word Word to save.
	 * @return Word the saved word.
	 * @throws RepositoryException if the insert fails.
	 */
	Word save(Word word) throws RepositoryException;

	/**
	 * Delete the table if it exists.
	 * 
	 * @throws RepositoryException if the table cannot be dropped.
	 */
	void deleteTable() throws RepositoryException;

	/**
	 * Return true if the table exists, false otherwise.
	 * 
	 * @return boolean true, the table exists, false it doesn't exist.
	 * @throws RepositoryException if the query can't be executed.
	 */
	boolean tableExists() throws RepositoryException;

	/**
	 * Delete a row with the given word as key.
	 * 
	 * @param key word to delete
	 * @throws RepositoryException if deletion fails
	 */
	void deleteRow(String key) throws RepositoryException;

	/**
	 * Delete all words from the table.
	 * 
	 * @throws RepositoryException if the query fails.
	 */
	void clearTable() throws RepositoryException;

	/**
	 * Save a word and its associated count.
	 * 
	 * @param word the word
	 * @param count the count, this should be a valid int
	 * @return a Word object containing the saved data.
	 * @throws RepositoryException if the insert fails.
	 */
	Word save(String word, int count) throws RepositoryException;

}