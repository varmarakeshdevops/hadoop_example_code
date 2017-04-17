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
public class HiveClient2 {

	private static final String DEFAULT_HIVE_SERVER_LOCATION = "localhost";
	private static final int DEFAULT_HIVE_SERVER_PORT = 10000;

	public static void testThriftClient(String hiveServerLocation, int hiveServerPort) throws TException {

		WordRepository wr = new WordRepository(hiveServerLocation,hiveServerPort,10000);
		
//		boolean exists = wr.tableExists();
//		System.out.println("Exists is "+exists);
//		wr.initTable();
//		exists = wr.tableExists();
//		System.out.println("Exists is "+exists);
		wr.save("foo","123");
		List<String> results = wr.findAll();
		System.out.println("Results are "+results);
		wr.close();
    }

	/**
	 * @param args
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
