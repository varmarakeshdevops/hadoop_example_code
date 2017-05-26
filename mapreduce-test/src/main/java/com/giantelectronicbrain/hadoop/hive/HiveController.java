package com.giantelectronicbrain.hadoop.hive;
 import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.mapreduce.JobRunner;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.giantelectronicbrain.hadoop.IWordRepository;
import com.giantelectronicbrain.hadoop.RepositoryException;
import com.giantelectronicbrain.hadoop.Word;

@RestController
@RequestMapping(value="/hive")
public class HiveController {
	
	private static final Log LOG = LogFactory.getLog(HiveController.class);
	
	@Autowired
	JobRunner hiveRunner;
	
	@Autowired
	IWordRepository hiveWordRepository;
	
	//http://localhost:8080/hive/startJob
	
	@RequestMapping(value = "/startJob", method = RequestMethod.POST, headers="Accept=application/json")
	Map<String, Object> startJobRunner() {
		Map<String, Object> dataMap = new HashMap<String, Object>();
		try {
			LOG.info("Creating a Hive table");
			hiveWordRepository.initTable();
			hiveWordRepository.clearTable();
			
			LOG.info("Performing map of HDFS data and push results to Hive");
			hiveRunner.call();
			
			dumpWordRepository(hiveWordRepository);
			dataMap.put("status", "1");
	        dataMap.put("statusMessage", "Hive JobRunner has been completed successfully");
		} catch (Exception e2) {
			e2.printStackTrace();
			dataMap.put("statusMessage", "JobRunner Failed");
		}
	        
		return dataMap;
	}
	
	private static void dumpWordRepository(IWordRepository wordRepository) throws RepositoryException {
		List<Word> words = wordRepository.findAll();
		words.sort(null);
		LOG.info("Number of Words = "+words.size());
		for(Word word : words) {
			System.out.println(word);
		}
	}
	   

}  

	
 