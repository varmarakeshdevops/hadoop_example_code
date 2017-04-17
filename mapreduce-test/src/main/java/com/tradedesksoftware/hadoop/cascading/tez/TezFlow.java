/**
 * 
 */
package com.tradedesksoftware.hadoop.cascading.tez;

/* import cascading.flow.FlowDef;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields; */

/**
 * @author tharter
 *
 */
public class TezFlow {
	//NOTE: need to figure out how to parameterize these, should be args to createFlowDef()
/*	private static final String DOCPATH = "/user/oracle/build/input";
	private static final String OUTPATH = "/user/oracle/build/output";
	
	public static FlowDef createFlowDef() {
		Tap inputTap = new Hfs(new TextDelimited(true,"\n"),DOCPATH);
		Tap outputTap = new Hfs(new TextDelimited(true,"\n"),OUTPATH);
		
		Fields token = new Fields("token");
		Fields text = new Fields("text");
		RegexSplitGenerator splitter = new RegexSplitGenerator(token,"[ \\[\\]\\(\\),.]");
		
		Pipe docPipe = new Each("token", text, splitter, Fields.RESULTS);
		
		Pipe wcPipe = new Pipe("wc");
		wcPipe = new GroupBy(wcPipe,token);
		wcPipe = new Every(wcPipe,Fields.ALL,new Count(), Fields.ALL);

		FlowDef flowDef = FlowDef.flowDef().setName("wc").addSource(docPipe, inputTap).addTailSink(wcPipe, outputTap);
		return flowDef;
	} */
}
