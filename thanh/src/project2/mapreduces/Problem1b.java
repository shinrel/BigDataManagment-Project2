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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import org.mortbay.log.Log;

import project2.obj.Point;
import project2.obj.Rectangle;
import org.apache.hadoop.mapred.JobConf;

public class Problem1b {
	private static final Logger logger = Logger.getLogger(Problem1b.class
			.getName());

	public static class MyMapperProblem1b extends
			Mapper<Object, Text, IntWritable, Text> {

		private Rectangle W;

		private final int SLICE_STEP = 1000; // divide the space 10000*10000 by
												// 100 continuous slices.
		// each slice is a 1000*1000 square.
		// First slice is <0, 0, 1000, 1000>, second slice is <0, 1000, 1000,
		// 2000>
		// 10th slice is <0, 9000, 1000, 10000>
		// 11th slice is <1000,0, 2000, 1000> and 20th slice is
		// <1000,9000,2000,10000> and so on

		private Vector<Rectangle> lstSlice = new Vector<>();

		public void generateSliceSquares(Rectangle W) {
			int numXSlice = Point.MAX_SCALE / SLICE_STEP;
			int numYSlice = Point.MAX_SCALE / SLICE_STEP;
			int[] x = new int[numXSlice];
			int[] y = new int[numYSlice];
			for (int i = 0; i < numXSlice; i++) {
				x[i] = i * SLICE_STEP; // generate 0, 1000, 2000, ...,9000
				y[i] = i * SLICE_STEP; // generate 0, 1000, 2000,... ,9000.
			}
			// now we got the bottom left points and x-coordinate is stored in
			// x[],
			// y- coordinate is stored in y[]

			// now we generate all slices. We fix height and weight by
			// SLICE_STEP.
			int recId = 0;
			for (int i = 0; i < numXSlice; i++) {
				for (int j = 0; j < numYSlice; j++) {
					Point bottomLeftPoint = new Point(x[i], y[j]);
					Point topRightPoint = new Point(x[i] + SLICE_STEP, y[j]
							+ SLICE_STEP);
					Rectangle rec = new Rectangle(recId, bottomLeftPoint,
							topRightPoint);
					if (rec.isOverlapRectangle(W)) {
						lstSlice.add(rec);
						recId += 1;
					}

				}
			}
		}

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			logger.info(context.getConfiguration().get("W"));
			W = Rectangle.parseFromString(context.getConfiguration().get("W"));
			logger.info(W.toString());
			generateSliceSquares(W);
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if (tokens.length == 5) {
				// rectangle file
				Rectangle recFile = Rectangle.parseFromString(value.toString());
				for (Rectangle slice : lstSlice) {
					if (slice.isOverlapRectangle(recFile)
							&& recFile.isOverlapRectangle(W)) {
						// consider only rectangles that overlap with W and
						// squared slices.
						context.write(new IntWritable(slice.getRecId()),
								new Text(recFile.toString()));
					}
				}
			} else if (tokens.length == 2) {
				// point file
				Point p = Point.parseFromString(value.toString());
				if (!W.containPoints(p))
					return; // if point p not in W --> return
				for (Rectangle slice : lstSlice) {
					if (slice.containPoints(p)) {
						context.write(new IntWritable(slice.getRecId()),
								new Text(p.toString()));
					}
				}
			}

		}
	}

	public static class MyReducerProblem1b extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			Vector<Point> points = new Vector<Point>();
			Vector<Rectangle> rectangles = new Vector<Rectangle>();
			for (Text val : values) {
				String s = val.toString();
				String[] tokens = s.split(",");
				if (tokens.length == 2) {
					// point
					points.add(Point.parseFromString(s));
				}
				if (tokens.length == 5) {
					// rectangle
					rectangles.add(Rectangle.parseFromString(s));
				}
			}
			// now merge points into rectangles
			for (Rectangle rec : rectangles) {
				for (Point p : points) {
					if (rec.containPoints(p)) {
						context.write(new IntWritable(rec.getRecId()),
								new Text(p.toStringIntWithBracket()));
					}
				}
			}

		}
	}

	public static void run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		System.out.println(otherArgs.length);
		if (otherArgs.length < 3) {
			System.err
					.println("Usage:  Problem1b [input_point] [input_rec] [output] [Window paramater (optional)]");
			System.out
					.println("Example /user/hadoop/input/point.txt /user/hadoop/input/rectangle.txt "
							+ "/user/hadoop/output/ 0,0,2000,2000");
			System.exit(1);
		}

		Rectangle W = null;
		if (args.length >= 4) {
			String line = "";
			for (int i = 3; i < args.length; i++) {
				line += args[i];
			}
			line = "0," + line; // adding ID of 0
			W = Rectangle.parseFromString(line);
			conf.set("W", W.toString());
			Log.info("Set W " + W.toString());

			Job job = new Job(conf, "Project 2, Problem 1b");
			job.setJarByClass(Problem1b.class); // change the class here
			job.setMapperClass(MyMapperProblem1b.class);
			// job.setCombinerClass(MyCombinerQuestion2d.class);
			job.setReducerClass(MyReducerProblem1b.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);

			job.setNumReduceTasks(1);

			for (int i = 0; i < otherArgs.length - 2; ++i) {
				Log.info("Input: " + otherArgs[i]);
				FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
			}
			Log.info("output: " + otherArgs[otherArgs.length - 2]);
			FileOutputFormat.setOutputPath(job, new Path(
					otherArgs[otherArgs.length - 2]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} else {
			Problem1a.run(args);
		}

	}
}
