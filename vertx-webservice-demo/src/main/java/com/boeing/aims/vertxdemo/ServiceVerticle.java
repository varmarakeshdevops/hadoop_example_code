/**
 * 
 */
package com.boeing.aims.vertxdemo;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;

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
		HttpServer server = vertx.createHttpServer();
		Router router = Router.router(vertx);
		
		router.route("/hadoop/sync/*").blockingHandler(routingContext -> {
			HttpServerRequest req = routingContext.request();
			String message = "";
			int status = 200;
			
			try {
				String command = req.path();

				message = "Launched: "+command+"\n";
				String[] cmdArgs = parseCommand(command);

				LOG.debug("Launching command: '"+command+"'");
				String output = hadoopLauncher.launch(cmdArgs);
				message += " '"+output+"'";
			} catch (Exception e) {
				message = "Failed: "+e.getMessage();
				status = 500;
				LOG.error("Failed to execute",e);
			}
			
			req.response()
				.putHeader("content-type", "text/plain")
				.setStatusCode(status)
				.end(message);
		});
		server.requestHandler(router::accept).listen(8080);
	}
	
	/**
	 * Turn a path component of a URL into a command plus parameters array
	 * suitable for calling HadoopLauncher.launch().
	 * 
	 * @param path path component of URL.
	 * @return String[] command arguments.
	 */
	private String[] parseCommand(String path) {
		String[] cmdArray = path.split("\\%20");
		String[] pathParts = cmdArray[0].split("/");
		cmdArray[0] = pathParts[pathParts.length-1];
		return cmdArray;
	}
}
