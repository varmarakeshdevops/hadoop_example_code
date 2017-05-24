/**
 * 
 */
package com.boeing.aims.vertxdemo;

import io.vertx.core.Verticle;
import io.vertx.core.Vertx;

/**
 * Application to deploy Vert.x service verticle.
 * 
 * @author tharter
 *
 */
public class VertexService {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.Log4j2LogDelegateFactory");
		Vertx vertx = Vertx.vertx();
		Verticle myVerticle = new ServiceVerticle();
		vertx.deployVerticle(myVerticle);
	}

}
