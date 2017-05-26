hadoop = Hadoop.login("https://localhost:8443/gateway/sandbox","guest","guest-password")
println Hdfs.rm(hadoop).file("/tmp/guest").recursive().now().string
println Hdfs.mkdir(hadoop).dir("/tmp/guest").now().string
println Hdfs.mkdir(hadoop).dir("/tmp/guest/input").now().string
println Hdfs.put(hadoop).file("/home/oracle/git/AIMS_infosys_code_share/hadoop_example_code/mapreduce-test/src/test/resources/input/itinerary.txt").to('/tmp/guest/input/itinerary.txt').now().string
println Hdfs.mkdir(hadoop).dir("/tmp/guest/jar").now().string
println Hdfs.put(hadoop).file("/home/oracle/git/AIMS_infosys_code_share/hadoop_example_code/mapreduce-test/build/libs/mapreduce-test.jar").to("/tmp/guest/jar/mapreduce-test.jar").now().string
println Hdfs.mkdir(hadoop).dir("/tmp/guest/lib").now().string
ldir = new File("/home/oracle/git/AIMS_infosys_code_share/hadoop_example_code/mapreduce-test/build/lib")
ldir.eachFile() {
	file -> Hdfs.put(hadoop).file(file.getAbsolutePath()).to("/tmp/guest/lib/"+file.getName()).now()
}
println Job.submitJava(hadoop).jar('/tmp/guest/jar/mapreduce-test.jar').app('com.giantelectronicbrain.hadoop.mapreduce.Driver').input('/tmp/guest/input').output('/tmp/guest/output').now().jobId
