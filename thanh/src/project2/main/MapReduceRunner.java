package project2.main;

import project2.mapreduces.Problem1a2;
import project2.mapreduces.Problem1b;

public class MapReduceRunner {
	public static void main(String[] args) throws Exception
	{
		String[] jobs = {"1a", "1b"};
		if (args.length < 1) {
			System.err.println("Usage [Job?] [input] [output]");
			System.out.println("Available jobs:");
			for (String job : jobs) {
				System.out.println(job);
			}
			System.exit(1);
		}
		String[] newArgs = new String[args.length - 1];
		for (int i = 1; i < args.length; i++) {
			newArgs[i - 1] = args[i];
		}
		if (args[0].equals(jobs[0])) {
			Problem1a2.run(newArgs);
		}
		if (args[0].equals(jobs[1])) {
			Problem1b.run(newArgs);
		}
	}
}
