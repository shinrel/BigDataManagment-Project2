package project2.obj;

import utils.Generator;

public class Point {
	private double x;
	private double y;
	public static final int MAX_SCALE = 10000;

	public static Point genRandomPoint() {
		double x = (double) Generator.genRanInt(1, MAX_SCALE);
		double y = (double) Generator.genRanInt(1, MAX_SCALE);
		return new Point(x, y);
	}

	public static Point genRandomPoint(int maxScale) {
		int x = Generator.genRanInt(1, maxScale);
		int y = Generator.genRanInt(1, maxScale);
		return new Point(x, y);
	}

	public boolean isInRectangle(Rectangle rec) {
		if (((this.getX() >= rec.getBottomLeftPoint().getX()) && (this.getX() <= rec
				.getTopRightPoint().getX()))
				&& ((this.getY() >= rec.getBottomLeftPoint().getY()) && (this
						.getY() <= rec.getTopRightPoint().getY()))) {
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
		newstr = newstr.replaceAll("\\s", "");
		String tokens[] = newstr.split(",");

		if (tokens.length != 2) {
			System.err.println("Error parsing to Point from string:" + s);
			System.exit(1);
		}
		try {
			double x = Double.parseDouble(tokens[0]);
			double y = Double.parseDouble(tokens[1]);
			return new Point(x, y);
		} catch (Exception ex) {
			int x = (int) Integer.parseInt(tokens[0]);
			int y = (int) Integer.parseInt(tokens[1]);
			return new Point(x, y);
		}
	}

	@Override
	public String toString() {
		return this.x + "," + this.y;
	}

	public String toStringInt() {
		return (int) this.x + "," + (int) this.y;
	}

	public String toStringWithBracket() {
		return "(" + this.x + "," + this.y + ")";
	}

	public String toStringIntWithBracket() {
		return "(" + this.x + "," + this.y + ")";
	}

	public Point(double x, double y) {
		super();
		this.x = x;
		this.y = y;
	}

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}

}
