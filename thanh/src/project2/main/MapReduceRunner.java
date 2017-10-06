package project2.main;

import project2.mapreduces.*;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Logger;

import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class MapReduceRunner {
	private static final Logger logger = Logger.getLogger("project2.main.MapReduceRunner");
	public static void main(String[] args) throws Exception
	{
//		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(conf);
//		Path outputPath = new Path("/user/hadoop/tmp/test.txt");
//		
//        if (fs.exists(outputPath)) {
//            System.err.println("output path exists");
//            return;
//        }
//        OutputStream os = fs.append(outputPath);
//        os.write("hello world".getBytes());
//        os.flush();
      
//        String path = "/user/hadoop/test.txt";
//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(java.net.URI.create(path), conf);
//        
////        Path newFolderPath= new Path(path);
////        if(!fs.exists(newFolderPath)) {
////            // Create new Directory
////            fs.mkdirs(newFolderPath);
////            logger.info("Path "+path+" created.");
////         }
//        logger.info("Begin Write file into hdfs");
//        //Create a path
//        Path hdfswritepath = new Path(path);
//        //Init output stream
//        FSDataOutputStream outputStream=fs.create(hdfswritepath, true);
//        //Cassical output stream usage
//        outputStream.writeBytes("hello world");
//        outputStream.close();
//        logger.info("End Write file into hdfs");
        
        
        
		String[] jobs = {"1a", "1b", "2",  "3.2.1",
				"3.2.2", "3.2.3", "3.2.4", "3.2.5"};
		if (args.length < 1) {
			System.err.println("Usage [Job?] [input] [output]");
			System.out.println("Available jobs:");
			for (String job : jobs) {
				System.out.println(job);
			}
			System.exit(1);
		}
		String[] newArgs = new String[args.length - 1];
		for (int i = 1; i < args.length; i++) {
			newArgs[i - 1] = args[i];
		}
		if (args[0].equals(jobs[0])) {
			Problem1a.run(newArgs);
		}
		if (args[0].equals(jobs[1])) {
			Problem1b.run(newArgs);
		}
		if (args[0].equals(jobs[2])) {
			Problem2.run(newArgs);
		}
		/*if (args[0].equals(jobs[3])) {
			System.out.println(newArgs.length);
			Problem3Step1.run(newArgs);
		}*/
		
		if (args[0].equals(jobs[3])) {
			//logger.info("Running Kmean 1");
			Problem3Kmean1.run(newArgs);
		}
		
		if (args[0].equals(jobs[4])) {
			Problem3Kmean2.run(newArgs);
		}
		if (args[0].equals(jobs[5])) {
			//logger.info("run kmean3");
			Problem3Kmean3.run(newArgs);
		}
		if (args[0].equals(jobs[6])) {
			Problem3Kmean4.run(newArgs);
		}
		if (args[0].equals(jobs[7])) {
			//for (String s : newArgs) logger.info(s);
			Problem3Kmean5.run(newArgs);
		}
		/*
		if (args[0].equals(jobs[9])) {
			Problem3Kmean6.run(newArgs);
		}*/
		
	}
}
