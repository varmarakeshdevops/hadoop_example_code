package com.giantelectronicbrain.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;


public class SparkConfig {
	

    @Value("${spark.app.name:SparkWordCountApp}")
    private String applName;

    @Value("${spark.home}")
    private String sparkHome;

    @Value("${spark.master.uri:local}")
    private String masterUri;
    
    
    
    private SparkConf sparkConf() {
        SparkConf sparkConf = new SparkConf()
                .setAppName(applName)
                .setSparkHome(sparkHome)
                .setMaster(masterUri);

        return sparkConf;
    }
    
    /**
     * Return JavaSparkContext
     * @author Latha
     * @return JavaSparkContext
     */
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(sparkConf());
    }
    
    public String getApplName() {
		return applName;
	}

	public void setApplName(String applName) {
		this.applName = applName;
	}

	public String getSparkHome() {
		return sparkHome;
	}

	public void setSparkHome(String sparkHome) {
		this.sparkHome = sparkHome;
	}

	public String getMasterUri() {
		return masterUri;
	}

	public void setMasterUri(String masterUri) {
		this.masterUri = masterUri;
	}

}
