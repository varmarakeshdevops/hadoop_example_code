package com.giantelectronicbrain.hadoop.mapreduce;
 import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.mapreduce.JobRunner;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value="/mapReduce")
public class MapReduceController {
	
	private static final Log LOG = LogFactory.getLog(MapReduceController.class);
	
	@Autowired
	JobRunner runner;
	
	//http://localhost:8080/mapReduce/startJob
	
	@RequestMapping(value = "/startJob", method = RequestMethod.POST, headers="Accept=application/json")
	Map<String, Object> startJobRunner() {
		Map<String, Object> dataMap = new HashMap<String, Object>();
		try {
			LOG.info("Performing basic HDFS file processing, outputs results to HDFS");
			
			runner.call();
			dataMap.put("status", "1");
	        dataMap.put("statusMessage", "MapReduce JobRunner has been completed successfully");
		} catch (Exception e2) {
			e2.printStackTrace();
			dataMap.put("statusMessage", "JobRunner Failed");
		}
	        
		return dataMap;
	}
	   

}  

	
 