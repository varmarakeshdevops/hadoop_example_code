package com.giantelectronicbrain.hadoop.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCloseOperationReq;
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

import com.giantelectronicbrain.hadoop.Word;

/**
 * Spring Repository stereotype to push words and counts into a Hive Table.
 * NOTE: This doesn't actually seem to work in the CDH5.9.0 environment, so
 * it hasn't been fully tested or wrapped into the other examples.
 * 
 * @author tharter
 *
 */
@org.springframework.stereotype.Repository
public class WordRepository {
	private static final Log LOG = LogFactory.getLog(WordRepository.class);
	private static final String DEFAULT_HIVE_SERVER_LOCATION = "localhost";
	private static final int DEFAULT_HIVE_SERVER_PORT = 10000;
	private static final int DEFAULT_HIVE_TIMEOUT = 10000;

	private String tableName = "words";
	private TCLIService.Client client;
	private TSessionHandle sessHandle;
	private TSocket transport;

	public WordRepository() throws TException {
		this(DEFAULT_HIVE_SERVER_LOCATION,DEFAULT_HIVE_SERVER_PORT,DEFAULT_HIVE_TIMEOUT);
	}
		
	public WordRepository(String hiveServerLocation,int hiveServerPort, int timeout) throws TException {
    	transport = new TSocket(hiveServerLocation, hiveServerPort);
    	transport.setTimeout(timeout);
    	
    	TBinaryProtocol protocol =  new TBinaryProtocol(transport);
    	client = new TCLIService.Client(protocol);
    	transport.open();
    	TOpenSessionReq openReq = new TOpenSessionReq();
    	TOpenSessionResp openResp = client.OpenSession(openReq);
    	sessHandle = openResp.getSessionHandle();
	}
	
	public void close() {
    	transport.close();
	}
	
	public List<String> findAll() throws TException {
		TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "SELECT * FROM "+tableName);
    	TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
    	TOperationHandle stmtHandle = execResp.getOperationHandle();

       	TFetchResultsReq fetchReq = new TFetchResultsReq(stmtHandle, TFetchOrientation.FETCH_FIRST, 100);
    	TFetchResultsResp resultsResp = client.FetchResults(fetchReq);

    	TRowSet resultsSet = resultsResp.getResults();
		List<TRow> resultRows = resultsSet.getRows();
		List<String> results = new ArrayList<String>();
    	for(TRow row : resultRows){
    	    results.add(row.toString());
    	}

    	TCloseOperationReq closeReq = new TCloseOperationReq();
    	closeReq.setOperationHandle(stmtHandle);
    	client.CloseOperation(closeReq);
    	
    	return results;
	}

	public void initTable() throws TException {
    	TOperationHandle stmtHandle = null;
		try {
			TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "CREATE TABLE IF NOT EXISTS "+tableName+" (count int, word string)");
	    	TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
	    	stmtHandle = execResp.getOperationHandle();
	    	if(execResp.getStatus().isSetErrorCode())
	    		throw new TException(execResp.getStatus().getErrorMessage());
	    } finally {
	    	if(stmtHandle != null) {
		    	TCloseOperationReq closeReq = new TCloseOperationReq();
		    	closeReq.setOperationHandle(stmtHandle);
		    	client.CloseOperation(closeReq);
	    	}
	    }
	}
	
	public Word save(Word word) throws TException {
		return save(word.getWord(),word.getCount());
	}
	
	public void deleteTable() throws TException {
		TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "DROP TABLE IF EXISTS "+tableName);
    	TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
    	TOperationHandle stmtHandle = execResp.getOperationHandle();
    	if(execResp.getStatus().isSetErrorCode()) {
    		throw new TException(execResp.getStatus().getErrorMessage());
    	}

    	TCloseOperationReq closeReq = new TCloseOperationReq();
    	closeReq.setOperationHandle(stmtHandle);
    	client.CloseOperation(closeReq);
	}
	
	public boolean tableExists() throws TException {
		TOperationHandle stmtHandle = null;
		try {
			TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "DESCRIBE "+tableName);
	    	TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
	    	if(execResp.getStatus().isSetErrorCode()) {
	    		String statusCode = execResp.getStatus().getSqlState();
	    		if("42S02".equals(statusCode)) {
	    			return false; // table doesn't exist, not an error.
	    		}
	    		throw new TException(execResp.getStatus().getErrorMessage());
	    	}
	    	stmtHandle = execResp.getOperationHandle();
	
	       	TFetchResultsReq fetchReq = new TFetchResultsReq(stmtHandle, TFetchOrientation.FETCH_FIRST, 1);
	    	TFetchResultsResp resultsResp = client.FetchResults(fetchReq);
		} finally {
			if(stmtHandle != null) {
		    	TCloseOperationReq closeReq = new TCloseOperationReq();
		    	closeReq.setOperationHandle(stmtHandle);
		    	client.CloseOperation(closeReq);
			}
		}
    	return true; // presumably if we get here, then the table exists...
	}

	/**
	 * Delete a row with the given word as key.
	 * 
	 * @param key word to delete
	 * @throws TException 
	 */
	public void deleteRow(String key) throws TException {
		TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "DELETE FROM "+tableName+" WHERE word = "+key);
    	TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
    	TOperationHandle stmtHandle = execResp.getOperationHandle();
    	if(execResp.getStatus().isSetErrorCode()) {
    		throw new TException(execResp.getStatus().getErrorMessage());
    	}

    	TCloseOperationReq closeReq = new TCloseOperationReq();
    	closeReq.setOperationHandle(stmtHandle);
    	client.CloseOperation(closeReq);
	}

	public void clearTable() throws TException {
		TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "DELETE FROM "+tableName);
    	TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
    	TOperationHandle stmtHandle = execResp.getOperationHandle();
    	if(execResp.getStatus().isSetErrorCode()) {
    		throw new TException(execResp.getStatus().getErrorMessage());
    	}

    	TCloseOperationReq closeReq = new TCloseOperationReq();
    	closeReq.setOperationHandle(stmtHandle);
    	client.CloseOperation(closeReq);
	}
	
	public Word save(final String word, final String count) throws TException {
    	TOperationHandle stmtHandle = null;
		Word w = new Word(word,count);
		try {
			TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "INSERT INTO TABLE "+tableName+" VALUES("+count+",'"+word+"')");
	    	TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
	    	stmtHandle = execResp.getOperationHandle();
	    	if(execResp.getStatus().isSetErrorCode()) {
	    		throw new TException(execResp.getStatus().getErrorMessage());
	    	}

		} finally {
			if(stmtHandle != null) {
		    	TCloseOperationReq closeReq = new TCloseOperationReq();
		    	closeReq.setOperationHandle(stmtHandle);
		    	client.CloseOperation(closeReq);
			}
		}
    	
		return w;
	}

}