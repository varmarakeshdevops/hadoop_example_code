package com.giantelectronicbrain.hadoop.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
import org.springframework.beans.factory.annotation.Autowired;

import com.giantelectronicbrain.hadoop.IWordRepository;
import com.giantelectronicbrain.hadoop.RepositoryException;
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
public class HiveWordRepository implements IWordRepository {
	private static final Log LOG = LogFactory.getLog(HiveWordRepository.class);
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
	public static final int DEFAULT_HIVE_TIMEOUT = 1000000;

	private String tableName = "words";
	
	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.hive.IWordRepository#getTableName()
	 */
	@Override
	public String getTableName() {
		return tableName;
	}

	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.hive.IWordRepository#setTableName(java.lang.String)
	 */
	@Override
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	private TCLIService.Client client;
	private TSessionHandle sessHandle;
	private TSocket transport;

	/**
	 * Construct a repository using defaults.
	 * 
	 * @throws RepositoryException if a connection cannot be established.
	 */
//	@Autowired
	public HiveWordRepository() throws RepositoryException {
		this(DEFAULT_HIVE_SERVER_LOCATION,DEFAULT_HIVE_SERVER_PORT,DEFAULT_HIVE_TIMEOUT);
	}
	
	/**
	 * Create a repository which connects to the given host and port with the given timeout.
	 * 
	 * @param hiveServerLocation host name to make thrift connection to.
	 * @param hiveServerPort port number to make thrift connection to.
	 * @param timeout thrift timeout.
	 * @throws RepositoryException if a connection cannot be established.
	 */
	@SuppressWarnings("unchecked")
	public HiveWordRepository(String hiveServerLocation,int hiveServerPort, int timeout) throws RepositoryException {
		try {
			LOG.debug("Creating thrift connection to "+hiveServerLocation+":"+hiveServerPort);
			transport = new TSocket(hiveServerLocation, hiveServerPort);
			transport.setTimeout(timeout);
			
			TBinaryProtocol protocol =  new TBinaryProtocol(transport);
			client = new TCLIService.Client(protocol);
			transport.open();
			TOpenSessionReq openReq = new TOpenSessionReq();
			@SuppressWarnings("rawtypes")
			Map configuration = new Properties();
			configuration.put("hive.txn.manager","org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
//			configuration.put("hive.compactor.initiator.on","true");
//			configuration.put("hive.compactor.worker.threads","1");
			configuration.put("hive.support.concurrency","true");
			configuration.put("hive.enforce.bucketing","true");
			configuration.put("hive.exec.dynamic.partition.mode","nonstrict");
			
			openReq.setConfiguration(configuration); // maybe can put setup stuff here...
			TOpenSessionResp openResp = client.OpenSession(openReq);
			sessHandle = openResp.getSessionHandle();
		} catch (TException te) {
			throw new RepositoryException(te.getMessage(),te);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.hive.IWordRepository#close()
	 */
	@Override
	public void close() {
    	transport.close();
	}
	
	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.hive.IWordRepository#findAll()
	 */
	@Override
	public List<Word> findAll() throws RepositoryException {
		try {
			TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "SELECT * FROM "+tableName);
			TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
			TOperationHandle stmtHandle = execResp.getOperationHandle();

			TFetchResultsReq fetchReq = new TFetchResultsReq(stmtHandle, TFetchOrientation.FETCH_FIRST, 100);
			TFetchResultsResp resultsResp = client.FetchResults(fetchReq);

			TRowSet resultsSet = resultsResp.getResults();
			List<TRow> resultRows = resultsSet.getRows();
			List<Word> results = new ArrayList<Word>();
			for(TRow row : resultRows){
				String w = (String) row.getFieldValue(row.fieldForId(0));
				Integer c = (Integer) row.getFieldValue(row.fieldForId(1));
				Word r = new Word(c,w);
			    results.add(r);
			}

			TCloseOperationReq closeReq = new TCloseOperationReq();
			closeReq.setOperationHandle(stmtHandle);
			client.CloseOperation(closeReq);
			
			return results;
		} catch (TException te) {
			throw new RepositoryException(te.getMessage(),te);
		}
	}

	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.hive.IWordRepository#initTable()
	 */
	@Override
	public void initTable() throws RepositoryException {
    	TOperationHandle stmtHandle = null;
		try {
			TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "CREATE TABLE IF NOT EXISTS "+tableName+" (count int, word string)"
					+ " CLUSTERED BY (word) INTO 2 BUCKETS STORED AS orc TBLPROPERTIES('transactional'='true')");
	    	TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
	    	stmtHandle = execResp.getOperationHandle();
	    	if(execResp.getStatus().isSetErrorCode())
	    		throw new TException(execResp.getStatus().getErrorMessage());
		} catch (TException te) {
			throw new RepositoryException(te.getMessage(),te);
	    } finally {
	    	if(stmtHandle != null) {
		    	TCloseOperationReq closeReq = new TCloseOperationReq();
		    	closeReq.setOperationHandle(stmtHandle);
		    	try {
					client.CloseOperation(closeReq);
				} catch (TException te) {
					throw new RepositoryException(te.getMessage(),te);
				}
	    	}
	    }
	}
	
	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.hive.IWordRepository#save(com.giantelectronicbrain.hadoop.Word)
	 */
	@Override
	public Word save(Word word) throws RepositoryException {
		return save(word.getWord(),word.getCount());
	}
	
	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.hive.IWordRepository#deleteTable()
	 */
	@Override
	public void deleteTable() throws RepositoryException {
		try {
			TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "DROP TABLE IF EXISTS "+tableName);
			TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
			TOperationHandle stmtHandle = execResp.getOperationHandle();
			if(execResp.getStatus().isSetErrorCode()) {
				throw new TException(execResp.getStatus().getErrorMessage());
			}

			TCloseOperationReq closeReq = new TCloseOperationReq();
			closeReq.setOperationHandle(stmtHandle);
			client.CloseOperation(closeReq);
		} catch (TException te) {
			throw new RepositoryException(te.getMessage(),te);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.hive.IWordRepository#tableExists()
	 */
	@Override
	public boolean tableExists() throws RepositoryException {
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
		} catch (TException te) {
			throw new RepositoryException(te.getMessage(),te);
		} finally {
			if(stmtHandle != null) {
		    	TCloseOperationReq closeReq = new TCloseOperationReq();
		    	closeReq.setOperationHandle(stmtHandle);
		    	try {
					client.CloseOperation(closeReq);
				} catch (TException te) {
					throw new RepositoryException(te.getMessage(),te);
				}
			}
		}
    	return true; // presumably if we get here, then the table exists...
	}

	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.hive.IWordRepository#deleteRow(java.lang.String)
	 */
	@Override
	public void deleteRow(String key) throws RepositoryException {
		try {
			TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "TRUNCATE TABLE IF EXISTS "+tableName);
			TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
			TOperationHandle stmtHandle = execResp.getOperationHandle();
			if(execResp.getStatus().isSetErrorCode()) {
				throw new TException(execResp.getStatus().getErrorMessage());
			}

			TCloseOperationReq closeReq = new TCloseOperationReq();
			closeReq.setOperationHandle(stmtHandle);
			client.CloseOperation(closeReq);
		} catch (TException te) {
			throw new RepositoryException(te.getMessage(),te);
		}
	}

	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.hive.IWordRepository#clearTable()
	 */
	@Override
	public void clearTable() throws RepositoryException {
		try {
			TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "DELETE FROM "+tableName);
			TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
			TOperationHandle stmtHandle = execResp.getOperationHandle();
			if(execResp.getStatus().isSetErrorCode()) {
				throw new TException(execResp.getStatus().getErrorMessage());
			}

			TCloseOperationReq closeReq = new TCloseOperationReq();
			closeReq.setOperationHandle(stmtHandle);
			client.CloseOperation(closeReq);
		} catch (TException te) {
			throw new RepositoryException(te.getMessage(),te);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.giantelectronicbrain.hadoop.hive.IWordRepository#save(java.lang.String, java.lang.String)
	 */
	@Override
	public Word save(final String word, final int count) throws RepositoryException {
    	TOperationHandle stmtHandle = null;
		Word w = new Word(count,word);
		try {
			TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "INSERT INTO TABLE "+tableName+" VALUES("+count+",'"+word+"')");
	    	TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
	    	stmtHandle = execResp.getOperationHandle();
	    	if(execResp.getStatus().isSetErrorCode()) {
	    		throw new TException(execResp.getStatus().getErrorMessage());
	    	}
		} catch (TException te) {
			throw new RepositoryException(te.getMessage(),te);
		} finally {
			if(stmtHandle != null) {
		    	TCloseOperationReq closeReq = new TCloseOperationReq();
		    	closeReq.setOperationHandle(stmtHandle);
		    	try {
					client.CloseOperation(closeReq);
				} catch (TException te) {
					throw new RepositoryException(te.getMessage(),te);
				}
			}
		}
    	
		return w;
	}

}