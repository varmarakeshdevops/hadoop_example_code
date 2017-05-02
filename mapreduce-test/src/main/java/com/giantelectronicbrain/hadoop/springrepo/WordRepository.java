/**
 * 
 */
package com.giantelectronicbrain.hadoop.springrepo;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import com.giantelectronicbrain.hadoop.IWordRepository;
import com.giantelectronicbrain.hadoop.RepositoryException;
import com.giantelectronicbrain.hadoop.Word;

/**
 * Provide stereotyped access to Words in any JPA-supported database.
 * 
 * @author tharter
 *
 */
@org.springframework.stereotype.Repository
public class WordRepository implements IWordRepository {

	@Autowired
	private JpaWordRepository jpaWordRepository;
	
	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.IWordRepository#getTableName()
	 */
	@Override
	public String getTableName() {
		return "words";
	}

	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.IWordRepository#setTableName(java.lang.String)
	 */
	@Override
	public void setTableName(String tableName) {
	}

	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.IWordRepository#close()
	 */
	@Override
	public void close() {
	}

	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.IWordRepository#findAll()
	 */
	@Override
	public List<Word> findAll() throws RepositoryException {
		return jpaWordRepository.findAll();
	}

	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.IWordRepository#initTable()
	 */
	@Override
	public void initTable() throws RepositoryException {
	}

	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.IWordRepository#save(com.giantelectronicbrain.hadoop.Word)
	 */
	@Override
	public Word save(Word word) throws RepositoryException {
		return jpaWordRepository.save(word);
	}

	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.IWordRepository#deleteTable()
	 */
	@Override
	public void deleteTable() throws RepositoryException {
	}

	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.IWordRepository#tableExists()
	 */
	@Override
	public boolean tableExists() throws RepositoryException {
		return true;
	}

	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.IWordRepository#deleteRow(java.lang.String)
	 */
	@Override
	public void deleteRow(String key) throws RepositoryException {
		jpaWordRepository.delete(key);
	}

	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.IWordRepository#clearTable()
	 */
	@Override
	public void clearTable() throws RepositoryException {
		jpaWordRepository.deleteAll();
	}

	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.IWordRepository#save(java.lang.String, java.lang.String)
	 */
	@Override
	public Word save(String word, int count) throws RepositoryException {
		Word w = new Word(count,word);
		return save(w);
	}

}
