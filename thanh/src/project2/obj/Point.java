package project2.obj;

import utils.Generator;

public class Point {
	private int x;
	private int y;
	public static final int MAX_SCALE = 10000;
	public static Point genRandomPoint()
	{
		int x = Generator.genRanInt(1, MAX_SCALE);
		int y = Generator.genRanInt(1, MAX_SCALE);
		return new Point(x, y);
	}
	public static Point genRandomPoint(int maxScale)
	{
		int x = Generator.genRanInt(1, maxScale);
		int y = Generator.genRanInt(1, maxScale);
		return new Point(x, y);
	}
	
	public boolean isInRectangle(Rectangle rec) {
		if (
				( 
					(this.getX() >= rec.getBottomLeftPoint().getX()) &&
					(this.getX() <= rec.getTopRightPoint().getX())
				) && 	
				(
					(this.getY() >= rec.getBottomLeftPoint().getY()) &&
					(this.getY() <= rec.getTopRightPoint().getY())	
				)
		){
			return true;
		}
		return false;
	}
	public static Point parseFromString(String s) {
		String newstr = s.replaceAll("\\(", "");
		newstr = newstr.replaceAll("\\)", "");
		newstr = newstr.replaceAll("<", "");
		newstr = newstr.replaceAll(">", "");
		newstr = newstr.replaceAll("\n", "");
		String tokens[] = newstr.split(",");
		
		if (tokens.length != 2) {
			System.err.println("Error parsing to Point from string:" + s);
			System.exit(1);
		}
		int x = Integer.parseInt(tokens[0]);
		int y = Integer.parseInt(tokens[1]);
		return new Point(x,y);
	}
	@Override
	public String toString() {
		return this.x + "," + this.y; 
	}
	
	public String toStringWithBracket() {
		return "(" + this.x + "," + this.y + ")"; 
	}
	
	public Point(int x, int y) {
		super();
		this.x = x;
		this.y = y;
	}
	public int getX() {
		return x;
	}
	public void setX(int x) {
		this.x = x;
	}
	public int getY() {
		return y;
	}
	public void setY(int y) {
		this.y = y;
	}
	
}
