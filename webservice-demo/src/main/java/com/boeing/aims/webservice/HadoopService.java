/**
 * 
 */
package com.boeing.aims.webservice;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;


/**
 * @author tharter
 *
 */
@Path("/hadoop")
public interface HadoopService {

	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/{commandLine}")
	public abstract String runHadoopCommand(@PathParam("commandLine")String commandLine);
}
