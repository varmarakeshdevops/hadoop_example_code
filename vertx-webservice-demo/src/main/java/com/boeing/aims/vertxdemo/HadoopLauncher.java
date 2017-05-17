/**
 * 
 */
package com.boeing.aims.vertxdemo;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Map;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Launch the hadoop command line application with the given parameters.
 *
 * @author tharter
 *
 */
public class HadoopLauncher {
	private static final Logger LOG = LoggerFactory.getLogger(HadoopLauncher.class);
	private CharsetDecoder decoder;
	private Charset cs;
	
	public HadoopLauncher() {
		cs = Charset.forName("utf8");
		decoder = cs.newDecoder();
	}

			
	public String launch(String command) throws IOException {
		LOG.trace("Processing command: '"+command+"'");
		String[] cmdArray = command.split("\\%20");
		LOG.trace("Command is: '"+cmdArray[0]+"'");
		ProcessBuilder pb = new ProcessBuilder();
		Map<String,String> env = pb.environment();
		env.put("HADOOP_CLASSPATH","build/lib/*:"+env.get("HADOOP_CLASSPATH"));
		pb.command(cmdArray);
		pb.redirectErrorStream(true);
		Process p = pb.start();
		InputStream stdout = p.getInputStream();
		StringBuilder output = new StringBuilder();
		byte[] buffer = new byte[1024];
		decoder.reset();
		while(stdout.read(buffer) != -1) {
			ByteBuffer in = ByteBuffer.wrap(buffer);
			CharBuffer more = decoder.decode(in);
			output.append(more);
		}
		return output.toString();
	}
}
