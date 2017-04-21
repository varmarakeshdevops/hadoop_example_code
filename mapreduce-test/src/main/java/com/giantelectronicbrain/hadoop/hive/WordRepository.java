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
	/**
	 * Default URL for Hive thrift connection.
	 */
	public static final String DEFAULT_HIVE_SERVER_LOCATION = "localhost";
	/**
	 * Default port for Hive thrift connection.
	 */
	public static final int DEFAULT_HIVE_SERVER_PORT = 10000;
	/**
	 * Default 10 second timeout for thrift network operations.
	 */
	public static final int DEFAULT_HIVE_TIMEOUT = 10000;

	private String tableName = "words";
	
	/**
	 * Get the name of the Hive table.
	 * 
	 * @return String hive table name
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * Set the name of the Hive table. Set this before you make queries.
	 * 
	 * @param tableName table name to use
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	private TCLIService.Client client;
	private TSessionHandle sessHandle;
	private TSocket transport;

	/**
	 * Construct a repository using defaults.
	 * 
	 * @throws TException if a connection cannot be established.
	 */
	public WordRepository() throws TException {
		this(DEFAULT_HIVE_SERVER_LOCATION,DEFAULT_HIVE_SERVER_PORT,DEFAULT_HIVE_TIMEOUT);
	}
	
	/**
	 * Create a repository which connects to the given host and port with the given timeout.
	 * 
	 * @param hiveServerLocation host name to make thrift connection to.
	 * @param hiveServerPort port number to make thrift connection to.
	 * @param timeout thrift timeout.
	 * @throws TException if a connection cannot be established.
	 */
	public WordRepository(String hiveServerLocation,int hiveServerPort, int timeout) throws TException {
		LOG.debug("Creating thrift connection to "+hiveServerLocation+":"+hiveServerPort);
    	transport = new TSocket(hiveServerLocation, hiveServerPort);
    	transport.setTimeout(timeout);
    	
    	TBinaryProtocol protocol =  new TBinaryProtocol(transport);
    	client = new TCLIService.Client(protocol);
    	transport.open();
    	TOpenSessionReq openReq = new TOpenSessionReq();
    	TOpenSessionResp openResp = client.OpenSession(openReq);
    	sessHandle = openResp.getSessionHandle();
	}
	
	/**
	 * Close the thrift connection. This should be used to clean up resources.
	 */
	public void close() {
    	transport.close();
	}
	
	/**
	 * Find all the words in the Hive table and return them all as a List. <b>Note:</b> this really
	 * won't scale well.
	 * 
	 * @return List&lt;String&gt; return records as a list in TSV form.
	 * 
	 * @throws TException if the query fails.
	 */
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

	/**
	 * Make sure that the table exists, create it if it doesn't.
	 * 
	 * @throws TException if the table cannot be created or there is some other Hive error.
	 */
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
	
	/**
	 * Save a word into the Hive table.
	 * 
	 * @param word Word to save.
	 * @return Word the saved word.
	 * @throws TException if the insert fails.
	 */
	public Word save(Word word) throws TException {
		return save(word.getWord(),word.getCount());
	}
	
	/**
	 * Delete the Hive table if it exists.
	 * 
	 * @throws TException if the table cannot be dropped.
	 */
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
	
	/**
	 * Return true if the table exists, false otherwise.
	 * 
	 * @return boolean true, the table exists, false it doesn't exist.
	 * @throws TException if the query can't be executed.
	 */
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
	    	LOG.debug("Result response for table existence query was "+resultsResp);
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
	 * @throws TException if deletion fails
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

	/**
	 * Delete all words from the table.
	 * 
	 * @throws TException if the query fails.
	 */
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
	
	/**
	 * Save a word and its associated count.
	 * 
	 * @param word the word
	 * @param count the count, this should be a valid int
	 * @return a Word object containing the saved data.
	 * @throws TException if the insert fails.
	 */
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