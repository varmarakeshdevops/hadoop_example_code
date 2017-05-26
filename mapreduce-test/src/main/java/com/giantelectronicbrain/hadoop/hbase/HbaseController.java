package com.giantelectronicbrain.hadoop.hbase;

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
import com.giantelectronicbrain.hadoop.Word;

@RestController
@RequestMapping(value="/hbase")
public class HbaseController {
	
private static final Log LOG = LogFactory.getLog(HbaseController.class);
	
	@Autowired
	JobRunner hbaseRunner;
	
	@Autowired
	IWordRepository hbaseWordRepository;
	
	//http://localhost:8080/hbase/startJob
	
	@RequestMapping(value = "/startJob", method = RequestMethod.POST, headers="Accept=application/json")
	Map<String, Object> startJobRunner() {
		Map<String, Object> dataMap = new HashMap<String, Object>();
		try {
			LOG.info("Initialization of Hbase table");
			hbaseWordRepository.initTable();
			LOG.info("-----Start Hbase JobRunner-------");
			hbaseRunner.call();
			LOG.info("HBaseOutput Application Running");
			
			List<Word> words = hbaseWordRepository.findAll();
			LOG.info("Number of Words = "+words.size());
			LOG.info(words);
			dataMap.put("status", "1");
	        dataMap.put("statusMessage", "Hbase JobRunner has been completed s	uccessfully");
		} catch (Exception e2) {
			e2.printStackTrace();
			dataMap.put("statusMessage", "JobRunner Failed");
		}
	        
		return dataMap;
	}

}
