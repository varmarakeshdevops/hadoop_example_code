package com.giantelectronicbrain.hadoop.controller;
 import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.giantelectronicbrain.hadoop.spark.WordCount;

@RestController
@RequestMapping(value="/word")
public class WordController {
	
	@Autowired
	WordCount wordCount;
	
	//http://localhost:8080/word/wordCount
	
	   @RequestMapping(value = "/wordCount", method = RequestMethod.POST, headers="Accept=application/json")
	    Map<String, Object> countWord() {
		   System.setProperty("hadoop.home.dir", "C:\\Users\\winutils");
	        Map<String, Object> dataMap = new HashMap<String, Object>();
	        wordCount.count();
	        dataMap.put("status", "1");
	        dataMap.put("statusMessage", "Successfully Inserted in Database");
		    return dataMap;
	    }

}  
 