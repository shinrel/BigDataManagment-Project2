package project2.obj;

import java.util.Vector;

import utils.Generator;

public class Rectangle {
	private static int ID = 1;
	//private static final String PREFIX = "r";
	private static final int MAX_HEIGHT = 20;
	private static final int MAX_WIDTH = 5;
	
	private int recId;
	private Point bottomLeftPoint;
	private Point topRightPoint;
	
	
	
	
	
	
	public Rectangle(int recId, Point bottomLeftPoint, Point topRightPoint) {
		super();
		this.recId = recId;
		this.bottomLeftPoint = bottomLeftPoint;
		this.topRightPoint = topRightPoint;
	}
	
	public static Rectangle genRectangle() {
		Point bottomLeftPoint = Point.genRandomPoint(Point.MAX_SCALE - 1);
		int randomHeight = 0;
		while (true){ 
			randomHeight = Generator.genRanInt(1, MAX_HEIGHT);
			if ((randomHeight + bottomLeftPoint.getY()) <= Point.MAX_SCALE) break;	
		}
		int topRightY = randomHeight + bottomLeftPoint.getY();
		int randomWidth = 0;
		while (true) {
			randomWidth = Generator.genRanInt(1, MAX_WIDTH);
			if ((randomWidth + bottomLeftPoint.getX()) <= Point.MAX_SCALE) break;
		}
		int topRightX = randomWidth + bottomLeftPoint.getX();
		Point topRightPoint = new Point(topRightX, topRightY);
		//Rectangle rec = new Rectangle(PREFIX + ID, bottomLeftPoint, topRightPoint);
		Rectangle rec = new Rectangle( ID, bottomLeftPoint, topRightPoint);
		ID += 1;
		return rec;
	}
	@Override
	public String toString() {
		return this.recId + "," + this.bottomLeftPoint.toString() + "," +
					this.topRightPoint.toString();
	}
	
	public boolean containPoints(Point point)
	{
		if (
				( 
					(point.getX() >= this.bottomLeftPoint.getX()) &&
					(point.getX() <= this.topRightPoint.getX())
				) && 	
				(
						(point.getY() >= this.bottomLeftPoint.getY()) &&
						(point.getY() <= this.topRightPoint.getY())	
				)
		){
			return true;
		}
		return false;
	}
	public boolean isOverlapRectangle(Rectangle rec)
	{
		//one rectangle is in the left of another
		if (this.getTopRightPoint().getX() < rec.getBottomLeftPoint().getX() ||
				this.getBottomLeftPoint().getX() > rec.getTopRightPoint().getX()) {
			return false;
		}
		//one rectangle is above another
		if ((this.getBottomLeftPoint().getY() > rec.getTopRightPoint().getY()) ||
				(this.getTopRightPoint().getY() < rec.getBottomLeftPoint().getY())) {
			return false;
		}
		return true;
	}
	
	public int getRecId() {
		return recId;
	}
	public void setRecId(int recId) {
		this.recId = recId;
	}
	public Point getBottomLeftPoint() {
		return bottomLeftPoint;
	}
	public void setBottomLeftPoint(Point bottomLeftPoint) {
		this.bottomLeftPoint = bottomLeftPoint;
	}
	public Point getTopRightPoint() {
		return topRightPoint;
	}
	public void setTopRightPoint(Point topRightPoint) {
		this.topRightPoint = topRightPoint;
	}
	
	public static Rectangle parseFromString(String s) {
		String newstr = s.replaceAll("\\(", "");
		newstr = newstr.replaceAll("\\)", "");
		newstr = newstr.replaceAll("<", "");
		newstr = newstr.replaceAll(">", "");
		newstr = newstr.replaceAll("\n", "");
		String tokens[] = newstr.split(",");
		if (tokens.length != 5) {
			System.err.println("Error parsing to Rectangle from string:" + s);
			System.exit(1);
			
		}
		int recId = Integer.parseInt(tokens[0]);
		int xbl = Integer.parseInt(tokens[1]);
		int ybl = Integer.parseInt(tokens[2]);
		int xtr = Integer.parseInt(tokens[3]);
		int ytr = Integer.parseInt(tokens[4]);
		return new Rectangle(recId, new Point(xbl, ybl), new Point(xtr, ytr));
	}
	
	public static void main(String[] args) {
		int SLICE_STEP = 1000;
		int numXSlice = Point.MAX_SCALE/SLICE_STEP;
		int numYSlice = Point.MAX_SCALE/SLICE_STEP;
		Vector<Rectangle> lstRecs = new Vector<>();
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
		for (Rectangle rec : lstRecs) {
			System.out.println(rec.toString());
		}
	}
	
	
	
	
}
