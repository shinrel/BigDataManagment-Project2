package project2.main;

import project2.obj.Point;
import project2.obj.Rectangle;
import utils.FileUtils;

public class Proj2Prob1DataGenerator  {
	public static void writeObjs(Object[] objs, String fileout) {
		if (FileUtils.checkExist(fileout)) {
			FileUtils.openFileAppend(fileout);
		} else {
			FileUtils.openFile(fileout);
		}
		int i = 0;
		for (Object obj : objs) {
			FileUtils.writeToFile(obj.toString());
			if (i ++ % 1000 == 0) {
				FileUtils.flushToFile();
			}
		}
		FileUtils.flushToFile();
		FileUtils.closeFile();
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.out.println("Usage: Proj2Prob1DataGenerator.jar [Point_MegaBytes] [Recs_MegaBytes] "
					+ " [point_directory_path] [rec_directory_path]");
			System.exit(1);
		}
		int pointsSizeMB = Integer.parseInt(args[0]);
		int recsSizeMB = Integer.parseInt(args[1]);
		String pointFilePath = args[2].endsWith("/") ?    (args[2] + "points.txt") : 
													"/" + (args[2] + "points.txt");
		String recFilePath = args[3].endsWith("/") ?      (args[3] + "rectangles.txt") :
													"/" + (args[3] + "rectangles.txt");
		
		int POINT_BATCH = 100000;
		int REC_BATCH = 10000;
		//gen random points
		while (FileUtils.checkFileSizeInMegaBytes(pointFilePath) < pointsSizeMB) {
			Point[] points = new Point[POINT_BATCH];
			for (int i = 0; i < POINT_BATCH; i++) {
				points[i] = Point.genRandomPoint();
			}
			//write all points to file
			writeObjs(points, pointFilePath);
			
			//clear object
			for (int i = 0; i < POINT_BATCH; i++) {
				points[i] = null;
			}
			points = null;
		}
		System.gc();
		
		//gen random rec:
		while (FileUtils.checkFileSizeInMegaBytes(recFilePath) < recsSizeMB) {
			Rectangle[] rectangles = new Rectangle[REC_BATCH];
			for (int i = 0; i < REC_BATCH; i++) {
				rectangles[i] = Rectangle.genRectangle();
			}
			//write all rectangles to file
			writeObjs(rectangles, recFilePath);
			//clear objects:
			for (int i = 0; i < REC_BATCH; i++) {
				rectangles[i] = null;
			}
			rectangles = null;
		}
		
		System.gc();
	}
}
