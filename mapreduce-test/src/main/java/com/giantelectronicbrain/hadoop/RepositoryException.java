/**
 * 
 */
package com.giantelectronicbrain.hadoop;

/**
 * Indicates an error while performing some operation in an IWordRepository.
 * 
 * @author tharter
 *
 */
public class RepositoryException extends Exception {

	private static final long serialVersionUID = -65974854323276233L;

	/**
	 *  Default constructor
	 */
	public RepositoryException() {
		super();
	}

	/**
	 * Create exception with a message.
	 * 
	 * @param message exception message
	 */
	public RepositoryException(String message) {
		super(message);
	}

	/**
	 * Create exception with a cause.
	 * 
	 * @param cause the cause.
	 */
	public RepositoryException(Throwable cause) {
		super(cause);
	}

	/**
	 * Create exception with cause and message.
	 * 
	 * @param message the message.
	 * @param cause the cause.
	 */
	public RepositoryException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * 
	 * @param message message
	 * @param cause cause
	 * @param enableSuppression true to enable suppression
	 * @param writableStackTrace true if stack trace should be writeable
	 */
	public RepositoryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
