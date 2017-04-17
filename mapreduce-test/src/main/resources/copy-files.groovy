//requires three variables, localSourceFile and inputDir, outputDir 

// use the shell (made available under variable fsh)

import org.apache.log4j.*;

Logger LOG = Logger.getLogger(getClass());
try {
    LOG.info("Source file is "+localSourceFile+" input dir is "+inputDir+" output dir is "+outputDir);
	if (!fsh.test(inputDir)) {
	   fsh.mkdir(inputDir); 
	   fsh.copyFromLocal(localSourceFile, inputDir); 
	   fsh.chmod(700, inputDir)
	}
	if (fsh.test(outputDir)) {
	   fsh.rmr(outputDir)
	}
	if (fsh.test(extraDir)) {
	   fsh.rmr(extraDir)
	}
} catch (Throwable e) {
	LOG.error("File copy script failed with error ",e);
	throw e;
}