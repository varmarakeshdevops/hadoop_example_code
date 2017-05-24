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
		Vertx vertx = Vertx.vertx();
		Verticle myVerticle = new ServiceVerticle();
		vertx.deployVerticle(myVerticle);
	}

}
