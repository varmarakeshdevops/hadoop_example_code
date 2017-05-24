/**
 * 
 */
package com.boeing.aims.webservice;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import io.swagger.annotations.Api;

/**
 * @author tharter
 *
 */
@Api("/hadoop")
@Service
public class HadoopServiceImpl implements HadoopService {
	private static final Logger LOG = Logger.getLogger(HadoopServiceImpl.class);
	
	@Override
	public String runHadoopCommand(@PathParam("commandLine")String commandLine) {
		LOG.info("GOT INVOKED WITH "+commandLine);
		return commandLine;
	}

}
