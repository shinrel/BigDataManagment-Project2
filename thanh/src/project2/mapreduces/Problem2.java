package project2.mapreduces;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.io.*;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Hashtable;
import java.util.logging.Logger;

import org.apache.hadoop.filecache.DistributedCache;

import project2.inputformat.JSONInputFormat;
import project2.inputformat.BKXmlRecordReader;
import project2.obj.Point;
import project2.obj.Problem2JsonObj;
import project2.obj.Rectangle;

public class Problem2 {
//	private static final Log LOG = LogFactory.getLog(Problem2.class.getName());
	private static final Logger logger = Logger.getLogger(Problem2.class.getName());
		public static class MyMapperProblem2 extends
			Mapper<Object, Text, IntWritable, IntWritable> {

		private IntWritable one = new IntWritable(1);
		 
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
//			logger.info("dumping value:" + value.toString());
			String[] tokens = value.toString().split(",");
			if (tokens.length != 13) {
				logger.info("Error parse json string:" + value.toString());
				return;
			} 
			
			int elevation = Integer.parseInt(tokens[8].split(":")[1].replaceAll("\\s", ""));
			
			//for counting each elevation valueInteger.parseInt(val);
			context.write(new IntWritable(elevation), one);
			context.write(new IntWritable(-2), one);
			
			//for calculating sum of all elevation values
			context.write(new IntWritable(-1), new IntWritable(elevation)); 
		}
	}


	public static class MyReducerProblem2 extends
			Reducer<IntWritable, IntWritable, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int totalCount = 0;
			
			for (IntWritable val : values) {
				totalCount += val.get();
			}
			if (key.get() == -1) {
				context.write(null, new Text("Aggregated Elevation: " + totalCount));
			} else if (key.get() == -2){
				context.write(null, new Text("Total objects: " + totalCount));
			} else {
				context.write(key, new Text(totalCount + ""));
			}
			
			
		}
	}

	public static void run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage:  Problem2 [input] [output] ");
			System.exit(2);
		}
		Job job = new Job(conf, "Project 2, Problem 2");
		job.setInputFormatClass(JSONInputFormat.class);
		job.setJarByClass(Problem2.class); // change the class here
		job.setMapperClass(MyMapperProblem2.class);
//		job.setCombinerClass(MyCombinerQuestion2d.class);
		
		job.setReducerClass(MyReducerProblem2.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
