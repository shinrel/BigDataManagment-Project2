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

import org.apache.hadoop.filecache.DistributedCache;

import project2.obj.Point;
import project2.obj.Rectangle;

public class Problem1a2 {
	public static class MyMapperProblem1a extends
			Mapper<Object, Text, IntWritable, Text> {

		private final int SLICE_STEP = 1000; //divide the space 10000*10000 by 100 continuous slices. 
		// each slice is a 1000*1000 square. 
		// First slice is <0, 0, 1000, 1000>, second slice is <1000, 0, 2000, 1000>
		// 10th slice is <9000, 0, 10000, 1000>
		// 11th slice is <0>
		private Vector<Rectangle> lstRecs = new Vector<>();
		public void generateSliceSquares()
		{
			int numXSlice = Point.MAX_SCALE/SLICE_STEP;
			int numYSlice = Point.MAX_SCALE/SLICE_STEP;
			int[] x = new int[numXSlice];
			int[] y = new int[numYSlice];
			for (int i = 0; i < numXSlice; i++) {
				x[i] = i*SLICE_STEP; //generate 0, 1000, 2000, ...,9000
				y[i] = i*SLICE_STEP; // generate 0, 1000, 2000,... ,9000.
			}
			//now we got the bottom left points and x-coordinate is stored in x[],
			//y- coordinate is stored in y[]
			
			//now we generate all slices. We fix height and weight by SLICE_STEP.
			int recId = 0;
			for (int i = 0; i < numXSlice; i++) {
				recId += 1;
				for (int j = 0; j < numYSlice; j++) {
					Point bottomLeftPoint = new Point(x[i], y[j]);
					Point topRightPoint = new Point(x[i], y[j] + SLICE_STEP);
					lstRecs.add(new Rectangle(recId, bottomLeftPoint, topRightPoint));
				}
			}
		}
		
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			generateSliceSquares();
		}
		

		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if (tokens.length == 5) {
				//rectangle file
				Rectangle recFile = Rectangle.parseFromString(value.toString());
				for (Rectangle rec : lstRecs) {
					if (rec.isOverlapRectangle(recFile)) {
						context.write(new IntWritable(rec.getRecId()), new Text(recFile.toString()));
					}
				}
			} else if(tokens.length == 2) {
				//point file
				Point p = Point.parseFromString(value.toString());
				for (Rectangle rec : lstRecs) {
					if (rec.containPoints(p)) {
						context.write(new IntWritable(rec.getRecId()), new Text(p.toString()));
					}
				}
			}
		
		}
	}


	public static class MyReducerProblem1a extends
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
					//point
					points.add(Point.parseFromString(s));
				}
				if (tokens.length == 5) {
					//rectangle
					rectangles.add(Rectangle.parseFromString(s));
				}
			}
			//now merge points into rectangles
			for (Rectangle rec : rectangles) {
				for (Point p : points) {
					if (rec.containPoints(p)) {
						context.write(new IntWritable(rec.getRecId()),
						 			new Text(p.toStringWithBracket()));
					}
				}
			}
			
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage:  Problem1a [input] [output] ");
			System.exit(2);
		}
		Job job = new Job(conf, "Project 2, Problem 1a");
		job.setJarByClass(Problem1a2.class); // change the class here
		job.setMapperClass(MyMapperProblem1a.class);
//		job.setCombinerClass(MyCombinerQuestion2d.class);
		job.setReducerClass(MyReducerProblem1a.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
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
