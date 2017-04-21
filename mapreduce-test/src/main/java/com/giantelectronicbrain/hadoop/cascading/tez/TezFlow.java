/**
 * 
 */
package com.giantelectronicbrain.hadoop.cascading.tez;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowRuntimeProps;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.tez.Hadoop2TezFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

/**
 * Build and execute a Tez-based Flow and execute it.
 * 
 * @author tharter
 *
 */
public class TezFlow {
	//NOTE: need to figure out how to parameterize these, should be args to createFlowDef()
	public static final String INPATH = "/user/oracle/build/input";
	public static final String OUTPATH = "/user/oracle/build/output";
	public static final String LOCALINPATH = "./src/test/resources/input/itinerary.txt";
	public static final String LOCALOUTPATH = "./build/output";

	private final String inputPath;
	private final String outputPath;
	private final Properties config;
	private final boolean isLocal;
	private FlowConnector flowConnector;
	private Tap<?, ?, ?> inputTap;
	private Tap<?, ?, ?> outputTap;

	/**
	 * Runs the default canned tez-based flow on the cluster. Pass false to run in local JVM
	 * for testing purposes.
	 * 
	 * @param isLocal if true use a local FlowConnector, otherwise connect to hadoop cluster.
	 */
	public static void DoTezFlow(boolean isLocal) {
		TezFlow tf = new TezFlow(isLocal);
		FlowDef fd = tf.createFlowDef();
		tf.runFlow(fd);
	}

	private void setFlowConnector(boolean isLocal) {
		if(isLocal) {
			flowConnector = new LocalFlowConnector(this.config);
		} else {
			flowConnector = new Hadoop2TezFlowConnector(this.config);			
		}
	}
	
	/**
	 * Instantiate with the given configuration and build Tez flowConnector that will execute the
	 * flow either locally or on the cluster. Note that the paths will be local in local mode and
	 * hdfs paths in cluster mode.
	 * 
	 * @param inputPath path to input file
	 * @param outputPath path to output directory
	 * @param isLocal true indicates we want to execute locally in the current JVM.
	 */
	public TezFlow(String inputPath, String outputPath, boolean isLocal) {
		this.inputPath = inputPath;
		this.outputPath = outputPath;
		this.isLocal = isLocal;
		this.config = createDefaultConfiguration();
		setFlowConnector(isLocal);
	}
	
	/**
	 * Create a basic default configuration for our flow. This is convenient as a way to 
	 * get a starting point if you want to create a custom config.
	 * 
	 * @return Properties java properties object
	 */
	public Properties createDefaultConfiguration() {
		Properties tprops = new Properties();
		AppProps.setApplicationJarClass(tprops, TezFlow.class);
		AppProps.setApplicationName(tprops, "Word Count Tez Example");
		return new FlowRuntimeProps().setGatherPartitions(1).setCombineSplits(true).buildProperties(tprops);
	}
	
	/**
	 * Instantiate with the given configuration and build Tez flowConnector that will execute the
	 * flow either locally or on the cluster. If isLocal is true then paths are HDFS, otherwise they
	 * are local file system paths.
	 * 
	 * @param inputPath the path to the input file
	 * @param outputPath the path to the output directory
	 * @param config properties to configure the flow
	 * @param isLocal true indicates we want to execute locally in the current JVM.
	 */
	public TezFlow(String inputPath, String outputPath, Properties config, boolean isLocal) {
		this.inputPath = inputPath;
		this.outputPath = outputPath;
		this.isLocal = isLocal;
		this.config = config;
		setFlowConnector(isLocal);
	}

	/**
	 * Instantiate with the given configuration and build a Tez flowConnector that will
	 * execute the resulting flow on the cluster.
	 * 
	 * @param inputPath the path to the input file in hdfs
	 * @param outputPath the path to the output hdfs directory
	 */
	public TezFlow(String inputPath, String outputPath) {
		this(inputPath, outputPath, false);
	}
	
	/**
	 * Construct instance that creates a default test flowConnector.
	 * 
	 * @param isLocal true if a local FlowConnector should be used.
	 */
	public TezFlow(boolean isLocal) {
		this(
				isLocal ? LOCALINPATH : INPATH,
				isLocal ? LOCALOUTPATH : OUTPATH,
				isLocal);
	}
	
	/**
	 * Run the given FlowDef with the configured connector.
	 * 
	 * @param fd FlowDef created by createFlowDef.
	 */
	public void runFlow(FlowDef fd) {
		Flow<?> flow = flowConnector.connect(fd);
		flow.complete();
	}
	
	private void makeTaps(Fields infields, Fields outfields) {
		if(isLocal) {
			inputTap = new FileTap(new cascading.scheme.local.TextLine(infields),inputPath);
			outputTap = new FileTap(new cascading.scheme.local.TextLine(outfields),outputPath);
		} else {
			inputTap = new Hfs(new TextLine(infields),inputPath);
			outputTap = new Hfs(new TextLine(outfields),outputPath);
		}
	}
	
	/**
	 * Create a FlowDef that will run a word count example flow.
	 * 
	 * @return FlowDef a flow definition to run the example input pipeline.
	 */
	public FlowDef createFlowDef() {
		Fields inputline = new Fields("inputline");
		Fields words = new Fields("words");
		RegexSplitGenerator splitter = new RegexSplitGenerator(inputline,"[ \\[\\]\\(\\),.]");
		Pipe docPipe = new Each("linepipe", inputline, splitter, Fields.RESULTS);
		Pipe wordPipe = new Pipe("wordpipe",docPipe);
		wordPipe = new GroupBy(wordPipe,Fields.ALL);
		wordPipe = new Every(wordPipe,Fields.ALL,new Count(), Fields.ALL);
		makeTaps(inputline,words);
		FlowDef flowDef = FlowDef.flowDef().setName("wc").addSource(docPipe,inputTap).addTailSink(wordPipe, outputTap);
		return flowDef;
	}
}
