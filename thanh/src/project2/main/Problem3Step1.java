package project2.main;

import project2.obj.Point;
import utils.FileUtils;

public class Problem3Step1 {
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage : [K] [output_K_seeds]");
			System.exit(1);
		}
		int K = Integer.parseInt(args[0]);
		FileUtils.openFile(args[1]);
		for (int i = 0; i < K; i++) {
			Point p = Point.genRandomPoint();
			FileUtils.writeToFile(p.toString());
		}
		FileUtils.flushToFile();
		FileUtils.closeFile();
	}
}
