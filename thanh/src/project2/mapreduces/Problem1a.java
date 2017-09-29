package project2.mapreduces;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Hashtable;

import org.apache.hadoop.filecache.DistributedCache;

import project2.obj.Point;
import project2.obj.Rectangle;

public class Problem1a {
	public static class MyMapperProblem1a extends
			Mapper<Object, Text, IntWritable, Text> {

		
		// store all rectangles in this lookup table
//		private Hashtable<String, String> lookupTbl = new Hashtable<String, String>();
		private Vector<Rectangle> recList = new Vector<Rectangle>();

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			
			Path[] uris = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			try {
				BufferedReader readBuffer = new BufferedReader(new FileReader(
						uris[0].toString()));
				String line;
				while ((line = readBuffer.readLine()) != null) {
					String[] tokens = line.split(",");
					if (tokens.length == 5) {
						Rectangle rec = Rectangle.parseFromString(line);
						recList.add(rec);
					} else {
						System.err.println("Can't work with " + line);
					}
					
				}
				readBuffer.close();
			} catch (Exception e) {
				System.out.println(e.toString());
			}
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			if (value.toString().split(",").length != 2)
				return;
			Point point = Point.parseFromString(value.toString());
			for (Rectangle rec : recList) {
				if(rec.containPoints(point)) {
					context.write(new IntWritable(rec.getRecId()), new Text(point.toStringWithBracket()));
				}
			}
		}
	}

//	public static class MyCombinerProblem1a extends
//			Reducer<IntWritable, Text, IntWritable, Text> {
//		private IntWritable result = new IntWritable();
//
//		public void reduce(IntWritable key, Iterable<Text> values,
//				Context context) throws IOException, InterruptedException {

//		}
//	}

	public static class MyReducerProblem1a extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
		}
	}

	public static void main(String[] args) throws Exception {
		long t1 = System.currentTimeMillis();
		final String NAME_NODE = "hdfs://localhost:8020";
		Configuration conf = new Configuration();
//		String[] otherArgs = new GenericOptionsParser(conf, args)
//				.getRemainingArgs();
		String[] otherArgs = args;
		if (otherArgs.length < 2) {
			System.err.println("Usage:  Problem1a [input] [output] ");
			System.exit(2);
		}
		Job job = new Job(conf, "Project 2, Problem 1a");
		job.setJarByClass(Problem1a.class); // change the class here
		job.setMapperClass(MyMapperProblem1a.class);
//		job.setCombinerClass(MyCombinerQuestion2d.class);
		job.setNumReduceTasks(0);
//		job.setNumReduceTasks(5);
//		job.setNumReduceTasks(10);
		
		job.setReducerClass(MyReducerProblem1a.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
//		if (args.length >= 3) {
//			job.setNumReduceTasks(Integer.parseInt(args[2]));
//		}

		String recPath = args[0].endsWith("/") ? args[0] + "rectangles.txt" :
												 args[0] + "/rectangles.txt";
		DistributedCache
				.addCacheFile(new URI(NAME_NODE
						+ recPath),
						job.getConfiguration());

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		long t2 = System.currentTimeMillis();
		System.out.println("Total time:" + (t2 -t1)/1000);
	}
}
