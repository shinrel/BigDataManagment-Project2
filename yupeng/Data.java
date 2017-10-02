import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

public class Data {
	public void createFile(String fileName){
		try{
		    String Input = fileName+"txt";
			PrintWriter writer = new PrintWriter(Input, "UTF-8");
		    
		} catch (IOException e) {System.out.println();}
	}
	protected String genRandomString(int Min, int Max) {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        int length = Min + (int)(Math.random() * ((Max - Min) + 1));
        
        while (salt.length() < length) {
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;
    }
	
	
	protected int genCtrCode(int Min, int Max){
		return Min + (int)(Math.random() * ((Max - Min) + 1));
	}
	
	
	protected void genMyPageFile() throws IOException{
		Data cus = new Data();
		cus.createFile("MyPage");
		BufferedWriter out = null;
		FileWriter fstream = new FileWriter("MyPage.txt", true);
		out = new BufferedWriter(fstream);
		try  
		{
			for (int i = 1; i <= 100000; i++){
				String temp = "";
				String name = cus.genRandomString(10,20);
				String Nationality = cus.genRandomString(10, 20);
				String Hobby = cus.genRandomString(10, 20);
				int CC = cus.genCtrCode(1, 10);
				temp = Integer.toString(i) + "," + name + "," + Nationality + "," 
				+ Integer.toString(CC) + "," + Hobby + "\n";
				out.write(temp);
			}
		   
		}
		catch (IOException e)
		{
		    System.err.println("Error: " + e.getMessage());
		}
		finally
		{
		    if(out != null) {
		        out.close();
		    }
		}
	}
	
	protected int genRandomID(int max){
		return 1 + (int)(Math.random() * (max));
	}
	
	protected int genRangeRandom(int min, int max) {
		return min + (int)(Math.random() * (max - min + 1));
	}
	
	protected void genFriendsFile() throws IOException{
		Data trs = new Data();
		trs.createFile("Friends");
		BufferedWriter out = null;
		FileWriter fstream = new FileWriter("Friends.txt", true);
		out = new BufferedWriter(fstream);
		try  
		{
			for (int i = 1; i <= 20000000; i++){
				String temp = "";
				int personId = trs.genRandomID(1000000);
				int myFriendId = trs.genRandomID(100000);
				int DateOfFri = trs.genRandomID(1000000);
				String Desc = trs.genRandomString(20, 50);
				temp = Integer.toString(i) + "," + Integer.toString(personId) + "," + Integer.toString(myFriendId) + "," 
				+ Integer.toString(DateOfFri) + "," + Desc + "\n";
				out.write(temp);
			}
		   
		}
		catch (IOException e)
		{
		    System.err.println("Error: " + e.getMessage());
		}
		finally
		{
		    if(out != null) {
		        out.close();
		    }
		}
	}
	protected void genAccessLogFile() throws IOException{
		Data acc = new Data();
		acc.createFile("AccessLog");
		BufferedWriter out = null;
		FileWriter fstream = new FileWriter("AccessLog.txt", true);
		out = new BufferedWriter(fstream);
		try  
		{
			for (int i = 1; i <= 10000000; i++){
				String temp = "";
				int byWho = acc.genRandomID(100000);
				int whatPage = acc.genRandomID(100000);
				String typeOfAcc = acc.genRandomString(20, 50);
				int accessTime = acc.genRandomID(1000000);
				temp = Integer.toString(i) + "," + Integer.toString(byWho) + "," + Integer.toString(whatPage) + "," 
				+ typeOfAcc + "," + Integer.toString(accessTime) + "\n";
				out.write(temp);
			}
		   
		}
		catch (IOException e)
		{
		    System.err.println("Error: " + e.getMessage());
		}
		finally
		{
		    if(out != null) {
		        out.close();
		    }
		}
	}
	protected void genPoints() throws IOException{
		Data cus = new Data();
		cus.createFile("Points");
		BufferedWriter out = null;
		FileWriter fstream = new FileWriter("Points.txt", true);
		out = new BufferedWriter(fstream);
		try  
		{
			for (int i = 1; i <= 10000000; i++){
				int x = genRangeRandom(1, 10000);
				int y = genRangeRandom(1, 10000);
				String temp = Integer.toString(i)+ ","+ Integer.toString(x) + "," + Integer.toString(y) + "\n";
				out.write(temp);
			}
		}
		catch (IOException e)
		{
		    System.err.println("Error: " + e.getMessage());
		}
		finally
		{
		    if(out != null) {
		        out.close();
		    }
		}
	}
	
	protected void genRectangles() throws IOException{
		Data cus = new Data();
		cus.createFile("Rectangles");
		BufferedWriter out = null;
		FileWriter fstream = new FileWriter("Rectangles.txt", true);
		out = new BufferedWriter(fstream);
		try  
		{
			for (int i = 1; i <= 100000; i++){
				int lenX = genRangeRandom(1, 20);
				int lenY = genRangeRandom(1, 5);
				int leftBottomX = genRangeRandom(1, 10000 - lenX);
				int leftBottomY = genRangeRandom(1, 10000 - lenY);
				int rightTopX = leftBottomX + lenX;
				int rightTopY = leftBottomY + lenY;
				String temp = Integer.toString(i)+ ","+ Integer.toString(leftBottomX)
				+ "," + Integer.toString(leftBottomY) + ","+Integer.toString(rightTopX) + "," 
				+ Integer.toString(rightTopY) + "\n";
				out.write(temp);
			}
		}
		catch (IOException e)
		{
		    System.err.println("Error: " + e.getMessage());
		}
		finally
		{
		    if(out != null) {
		        out.close();
		    }
		}
	}
	public static void main(String args[]) throws IOException{
		Data data = new Data();
		data.genPoints();
		data.genRectangles();
	}
}
	

