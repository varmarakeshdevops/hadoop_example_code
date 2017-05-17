package com.giantelectronicbrain.hadoop.dao;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.mongodb.core.MongoOperations;

import com.giantelectronicbrain.hadoop.model.Word;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class WordDAOImpl implements WordDAO {
	
	private MongoOperations mongoOps;
	
	private static final String WORD_COLLECTION = "Word";
	
	private DBCollection getDBCollection(){
		return this.mongoOps.getCollection(WORD_COLLECTION);
	}
	
	public WordDAOImpl(){
		
	}
	
	public WordDAOImpl(MongoOperations mongoOps){
		this.mongoOps=mongoOps;
	}
	
	@Override
	public void create(Word w) {
		this.mongoOps.insert(w, WORD_COLLECTION);

	}

	@Override
	public void update(Word w) {
		DBCollection  coll = getDBCollection();
		DBObject query = new BasicDBObject("word", w.getWord());
		//delete existing record.
		coll.findAndRemove(query);
		// insert new object with updates
		this.mongoOps.insert(w, WORD_COLLECTION);

	}

	@Override
	public void deleteByWord(String word) {
		DBCollection  coll = getDBCollection();
		DBObject query = new BasicDBObject("word", word);
		DBObject obj = coll.findAndRemove(query);
	}

	@Override
	public Word findByWord(String word) {
		DBCollection  coll = getDBCollection();
		// creating new object
		DBObject query = new BasicDBObject("word", word);
		DBObject d1 = coll.findOne(query);
		Word w =  this.mongoOps.getConverter().read(Word.class, d1);
		return w;
	}

	@Override
	public List<Word> findAll() {
		DBCollection  coll = getDBCollection();
		DBCursor cursor = coll.find();
		List<Word> wordList = new ArrayList<Word>();	
		while (cursor.hasNext()) { 
		    DBObject obj = cursor.next(); 
		    Word w = this.mongoOps.getConverter().read(Word.class, obj);  
		    wordList.add(w); 
		}
		return wordList;
	}

}
