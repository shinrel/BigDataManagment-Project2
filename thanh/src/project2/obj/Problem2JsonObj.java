package project2.obj;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class Problem2JsonObj {
	private static final Log LOG = LogFactory.getLog(Problem2JsonObj.class.getName());
	
	public String ID;
	public String ShortName;
	public String Name;
	public String Region;
	public String ICAO;
	public int Flags;
	public int Catalog;
	public int Length;
	public int Elevation;
	public String Runway;
	public int Frequency;
	public String Latitude;
	public String Longitude;
	
	
	
	public Problem2JsonObj() {
		super();
	}
	
	public static Problem2JsonObj parse(String jsonStr) {
		try {
			return new ObjectMapper().readValue(jsonStr, Problem2JsonObj.class);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			LOG.error("Parse error with jsonStr:" + jsonStr);
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			LOG.error("Parse error with jsonStr:" + jsonStr);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("Parse error with jsonStr:" + jsonStr);
		}
		return null;
	}
	public Problem2JsonObj(String iD, String shortName, String name,
			String region, String iCAO, int flags, int catalog, int length,
			int elevation, String runway, int frequency, String latitude,
			String longitude) {
		super();
		ID = iD;
		ShortName = shortName;
		Name = name;
		Region = region;
		ICAO = iCAO;
		Flags = flags;
		Catalog = catalog;
		Length = length;
		Elevation = elevation;
		Runway = runway;
		Frequency = frequency;
		Latitude = latitude;
		Longitude = longitude;
	}
	@Override
	public String toString() {
		return "ID:" + this.ID + "," +
				"ShortName:" + this.ShortName + "," + 
		"Name:" + this.Name + "," +
		"Region:" + this.Region + "," +
		"ICAO:" + this.ICAO + "," +
		"Flags:" + this.Flags + "," +
		"Catalog:" + this.Catalog + "," +
		"Length:" + this.Length + "," +
		"Elevation:" + this.Elevation + "," +
		"Runway:" + this.Runway + "," +
		"Frequency:" + this.Frequency + "," +
		"Latitude:" + this.Latitude + "," +
		"Longitude:" + this.Longitude
		;
	}
	
	public String getID() {
		return ID;
	}
	public void setID(String iD) {
		ID = iD;
	}
	public String getShortName() {
		return ShortName;
	}
	public void setShortName(String shortName) {
		ShortName = shortName;
	}
	public String getName() {
		return Name;
	}
	public void setName(String name) {
		Name = name;
	}
	public String getRegion() {
		return Region;
	}
	public void setRegion(String region) {
		Region = region;
	}
	public String getICAO() {
		return ICAO;
	}
	public void setICAO(String iCAO) {
		ICAO = iCAO;
	}
	public int getFlags() {
		return Flags;
	}
	public void setFlags(int flags) {
		Flags = flags;
	}
	public int getCatalog() {
		return Catalog;
	}
	public void setCatalog(int catalog) {
		Catalog = catalog;
	}
	public int getLength() {
		return Length;
	}
	public void setLength(int length) {
		Length = length;
	}
	public int getElevation() {
		return Elevation;
	}
	public void setElevation(int elevation) {
		Elevation = elevation;
	}
	public String getRunway() {
		return Runway;
	}
	public void setRunway(String runway) {
		Runway = runway;
	}
	public int getFrequency() {
		return Frequency;
	}
	public void setFrequency(int frequency) {
		Frequency = frequency;
	}
	public String getLatitude() {
		return Latitude;
	}
	public void setLatitude(String latitude) {
		Latitude = latitude;
	}
	public String getLongitude() {
		return Longitude;
	}
	public void setLongitude(String longitude) {
		Longitude = longitude;
	}
	
	public static void main(String[] arsg) throws JsonParseException, JsonMappingException, IOException {
		String jsonStr = "{" 
				+ "\"ID\": \"LFOI\","
				+ "\"ShortName\": \"ABBEV\","
				+ "\"Name\": \"ABBEVILLE\","
				+ "\"Region\": \"FR\","
				+ "\"ICAO\": \"LFOI\","
				+ "\"Flags\": 72,"
				+ "\"Catalog\": 0,"
				+ "\"Length\": 1260,"
				+ "\"Elevation\": 67,"
				+ "\"Runway\": \"0213\","
				+ "\"Frequency\": 0,"
				+ "\"Latitude\": \"N500835\","
				+ "\"Longitude\": \"E0014954\"}";
		System.out.println(jsonStr);
		Problem2JsonObj jsonObj = new ObjectMapper().readValue(jsonStr, Problem2JsonObj.class);
		System.out.println(jsonObj.toString());
	}
		
}
