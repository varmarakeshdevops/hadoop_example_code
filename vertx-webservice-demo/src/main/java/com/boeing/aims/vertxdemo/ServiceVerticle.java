/**
 * 
 */
package com.boeing.aims.vertxdemo;

import java.io.IOException;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
/**
 * Vertx service Verticle which sets up service handler.
 * 
 * @author tharter
 *
 */
public class ServiceVerticle extends AbstractVerticle {
	private static final Logger LOG = LoggerFactory.getLogger(ServiceVerticle.class);
	
	private HadoopLauncher hadoopLauncher = new HadoopLauncher();
	
	public void start() {
		LOG.info("Starting Service Verticle");
		HttpServer server = vertx.createHttpServer().requestHandler(req -> {
			String message = "";
			int status = 200;
			
			try {
				String command = req.path();

				String[] elements = command.split("/");
				command = elements[elements.length-1];
				message = "Launched: "+command;

				LOG.debug("Launching command: '"+command+"'");
				String output = hadoopLauncher.launch(command);
				message += ", '"+output+"'";
			} catch (Exception e) {
				message = "Failed: "+e.getMessage();
				LOG.error("Failed to execute",e);
			}
			
			req.response()
				.putHeader("content-type", "text/plain")
				.setStatusCode(status)
				.end(message);
		});
		server.listen(8080);
	}
}
