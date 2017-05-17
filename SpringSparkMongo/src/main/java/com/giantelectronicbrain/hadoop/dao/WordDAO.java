package com.giantelectronicbrain.hadoop.dao;

import java.util.List;

import com.giantelectronicbrain.hadoop.model.Word;

public interface WordDAO {
	
	public void create(Word w);
	
	public void update(Word w);
	
	public void deleteByWord(String word);	
	
	public Word findByWord(String word);
	
	public List<Word> findAll();

}
