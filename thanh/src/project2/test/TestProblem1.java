package project2.test;

import java.util.Vector;

import project2.obj.Point;
import project2.obj.Rectangle;
import utils.FileUtils;

public class TestProblem1 {
	public static void main(String[] args) {
//		args = new String[4];
//		args[0] = "datasets/points.txt";
//		args[1] = "datasets/rectangles.txt";
//		args[2] = "datasets/merged_result_1b.txt";
//		args[3] = "0,10000,0,10000";
		if (args.length < 3) {
			System.err.println("Usage: [input_point_file] [input_rectangle_file] [output_file]"
					+ " [Window parameter W]");
			System.out.println("Window parameter is in format (no space in between coordinates)"
					+ "x-bottom-left,y-bottom-left,x-top-right,y-top-right");
			System.out.println("Example :  points.txt rectangles.txt merged_result.txt 0,0,2000,2000");
			System.exit(1);
		}
		
		String inputPointFile = args[0];
		String inputRecFile = args[1];
		String outputFile = args[2];
		Rectangle W = null;
		if (args.length >= 4) {
			String line = "";
			for (int i = 3; i < args.length; i++) {
				line += args[i];
			}
			line = "0," + line; //adding ID of 0
			W = Rectangle.parseFromString(line);
		}
		System.out.println("Input point file is: " + inputPointFile);
		System.out.println("Input rectangle file is: " + inputRecFile);
		System.out.println("Output file is: " + outputFile);
		if (W  != null) System.out.println("Window parameter is: " + W.toStringWithoutID());
		
		//read points 
		Vector<Point> points = new Vector<>();
		for (String line : FileUtils.readContentFromFile(inputPointFile)) {
			Point point = Point.parseFromString(line);
			if (W != null && !W.containPoints(point)) continue;
			points.add(point);
		}
		
		//read rectangles
		Vector<Rectangle> recs = new Vector<>();
		for (String line : FileUtils.readContentFromFile(inputRecFile)) {
			Rectangle rec = Rectangle.parseFromString(line);
			if (W != null && !W.isOverlapRectangle(rec)) continue;
			recs.add(rec);
		}
		
		FileUtils.openFile(outputFile);
		int i = 0;
		for (Rectangle rec : recs) {
			for (Point point : points) {
				if (rec.containPoints(point)) {
					i ++;
					String line = "" + rec.getRecId() + "," + point.toStringWithBracket();
					FileUtils.writeToFile(line);
					if (i%1000 == 0) FileUtils.flushToFile();
				}
			}
		}
		FileUtils.flushToFile();
		FileUtils.closeFile();
	}
}
