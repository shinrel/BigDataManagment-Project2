package project2.mapreduces;

import java.io.BufferedReader;
import java.io.FileReader;
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
import utils.Utils;

public class Problem3Kmean2 {
	// private static final Log LOG =
	// LogFactory.getLog(Problem2.class.getName());
	private static final Logger logger = Logger.getLogger(Problem3Kmean2.class
			.getName());

	public static class MyMapperProblem3Kmean extends
			Mapper<Object, Text, IntWritable, Text> {

		private Vector<Point> centers = new Vector<>();

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {

			// reading k-seeds
			Path[] uris = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			try {
				BufferedReader readBuffer = new BufferedReader(new FileReader(
						uris[0].toString()));
				String line;
				while ((line = readBuffer.readLine()) != null) {
					if (line.split(",").length != 2) {
						logger.info("Wrong line : " + line);
						continue;
					}
					line = line.replaceAll("\\s", "");
					logger.info(line);
					Point p = Point.parseFromString(line);
					centers.add(p);
				}
				readBuffer.close();
			} catch (Exception e) {
				System.out.println(e.toString());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String s = value.toString();
			Point p = Point.parseFromString(s);
		
			int closestCenterIdx = Utils.getClosestPointIdx(p, centers);
			context.write(new IntWritable(closestCenterIdx),
					new Text(p.toString()));
		}
	}

	public static class MyReducerProblem3Kmean extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {

		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

		}

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			double new_x = 0.0;
			double new_y = 0.0;
			int totalPoint = 0;
			for (Text val : values) {
				Point p = Point.parseFromString(val.toString());
				new_x += p.getX();
				new_y += p.getY();
				totalPoint++;
			}
			// average here
			new_x = new_x / totalPoint;
			new_y = new_y / totalPoint;
			context.write(null, new Text(new_x + "," + new_y));
		}
	}

	public static void run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 4) {
			System.err
					.println("Usage:  Problem2 [initial-K-seed-path] [input-points-path] [output] [max-iter] [distance-threshold]");
			System.exit(2);
		}
		// conf.set("centers-path", otherArgs[0]);
		int max_iter = Integer.parseInt(otherArgs[3]);
		for (int i = 0; i < max_iter; i++) {

			Job job = new Job(conf, "Project 3, Problem 2");
			// conf.set("kSeedPath", otherArgs[0]);

			job.setJarByClass(Problem3Kmean2.class); // change the class here
			job.setMapperClass(MyMapperProblem3Kmean.class);
			// job.setCombinerClass(MyCombinerQuestion2d.class);

			job.setReducerClass(MyReducerProblem3Kmean.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);

			// job.setNumReduceTasks(5);
			// final String NAME_NODE = "hdfs://localhost:8020";
			final String NAME_NODE = "";
			if (i == 0) {
				DistributedCache.addCacheFile(
						new URI(NAME_NODE + otherArgs[0]),
						job.getConfiguration());
			} else {
				String path = otherArgs[2].endsWith("/") ? otherArgs[2]
						.substring(0, otherArgs[2].length() - 2) : otherArgs[2];
						
				
				String newPath = NAME_NODE + path + (i - 1) + "/part-r-00000";
				logger.info(newPath);
				DistributedCache.addCacheFile(new URI(newPath),
						job.getConfiguration());
			}

			FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job,
					new Path(otherArgs[2].endsWith(i + "") ? (otherArgs[2])
							: (otherArgs[2] + i)));
			boolean res = job.waitForCompletion(true);
		}
		// System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
