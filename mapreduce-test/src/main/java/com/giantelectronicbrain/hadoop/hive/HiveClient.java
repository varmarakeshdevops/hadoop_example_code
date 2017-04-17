/**
 * 
 */
package com.giantelectronicbrain.hadoop.hive;

import java.util.List;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCloseOperationReq;
import org.apache.hive.service.cli.thrift.TCloseSessionReq;
import org.apache.hive.service.cli.thrift.TColumn;
import org.apache.hive.service.cli.thrift.TExecuteStatementReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementResp;
import org.apache.hive.service.cli.thrift.TFetchOrientation;
import org.apache.hive.service.cli.thrift.TFetchResultsReq;
import org.apache.hive.service.cli.thrift.TFetchResultsResp;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

/**
 * Basic tests of talking to Hive using Thrift.
 * 
 * @author tharter
 *
 */
public class HiveClient {

	private static final String DEFAULT_HIVE_SERVER_LOCATION = "localhost";
	private static final int DEFAULT_HIVE_SERVER_PORT = 10000;

	public static void testThriftClient(String hiveServerLocation, int hiveServerPort) throws TException {

    	TSocket transport = new TSocket(hiveServerLocation, hiveServerPort);

    	transport.setTimeout(10000);
    	TBinaryProtocol protocol =  new TBinaryProtocol(transport);
    	TCLIService.Client client = new TCLIService.Client(protocol);  
    	transport.open();
    	TOpenSessionReq openReq = new TOpenSessionReq();
    	TOpenSessionResp openResp = client.OpenSession(openReq);
    	TSessionHandle sessHandle = openResp.getSessionHandle();

    	TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "SHOW TABLES");
    	TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
    	TOperationHandle stmtHandle = execResp.getOperationHandle();

    	TFetchResultsReq fetchReq = new TFetchResultsReq(stmtHandle, TFetchOrientation.FETCH_FIRST, 100);
    	TFetchResultsResp resultsResp = client.FetchResults(fetchReq);

    	TRowSet resultsSet = resultsResp.getResults();
		List<TColumn> resultCols = resultsSet.getColumns();
		TColumn firstCol = resultCols.get(0);
		List<String> cnames = firstCol.getStringVal().getValues();
    	for(String cname : cnames){
    	    System.out.println(cname);
    	}

    	TCloseOperationReq closeReq = new TCloseOperationReq();
    	closeReq.setOperationHandle(stmtHandle);
    	client.CloseOperation(closeReq);
    	
    	execReq = new TExecuteStatementReq(sessHandle, "SELECT * FROM cust");
    	execResp = client.ExecuteStatement(execReq);
    	stmtHandle = execResp.getOperationHandle();

    	fetchReq = new TFetchResultsReq(stmtHandle, TFetchOrientation.FETCH_FIRST, 100);
    	resultsResp = client.FetchResults(fetchReq);

    	resultsSet = resultsResp.getResults();
		List<TRow> resultRows = resultsSet.getRows();
		System.out.println("got "+resultRows.size()+" rows");
    	for(TRow row : resultRows){
    	    System.out.println(row.toString());
    	}

    	closeReq = new TCloseOperationReq();
    	closeReq.setOperationHandle(stmtHandle);
    	client.CloseOperation(closeReq);
    	
    	
    	TCloseSessionReq closeConnectionReq = new TCloseSessionReq(sessHandle);
    	client.CloseSession(closeConnectionReq);

    	transport.close();
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
		}
	}

}
