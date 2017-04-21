/**
 * 
 */
package com.giantelectronicbrain.hadoop.hive;

import java.util.List;

import org.apache.thrift.TException;

/**
 * Basic talking to Hive via a Hive version of WordRepository.
 * 
 * @author tharter
 *
 */
public class HiveClient {

	private static final String DEFAULT_HIVE_SERVER_LOCATION = "localhost";
	private static final int DEFAULT_HIVE_SERVER_PORT = 10000;

	/**
	 * Perform some basic operations using a WordRepository. This will just save a word and then
	 * print out a dump of the contents of the table.
	 * 
	 * @param hiveServerLocation hostname of hive server
	 * @param hiveServerPort port of hive server
	 * @throws TException if something fails.
	 */
	public static void testThriftClient(String hiveServerLocation, int hiveServerPort) throws TException {

		WordRepository wr = new WordRepository(hiveServerLocation,hiveServerPort,10000);
		
		wr.save("foo","123");
		List<String> results = wr.findAll();
		System.out.println("Results are "+results);
		wr.close();
    }

	/**
	 * Accepts a hostname and port, connects to the given Hive server via thrift, and 
	 * tries some basic operations. If no arguments are supplied, defaults will be used.
	 * 
	 * @param args command line arguments.
	 */
	public static void main(String[] args) {
		String hiveServerLocation = DEFAULT_HIVE_SERVER_LOCATION;
		if(args.length > 0) hiveServerLocation = args[0];
		int hiveServerPort = DEFAULT_HIVE_SERVER_PORT;
		if(args.length > 1) {
			String hsPortStr = args[1];
			hiveServerPort = Integer.valueOf(hsPortStr);
		}
		
		try {
			System.out.println("Hive Test Client "+hiveServerLocation+":"+hiveServerPort);
			testThriftClient(hiveServerLocation,hiveServerPort);
		} catch (TException e) {
			e.printStackTrace();
			System.out.println("what is the cause"+e.getCause());
		}
	}

}
