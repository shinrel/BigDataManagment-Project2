package project2.inputformat;

import java.io.IOException;
import java.util.logging.Logger;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.mortbay.log.Log;

public class BKXmlRecordReader implements RecordReader<LongWritable,Text> {
	private final byte[] startXMLTag = "{".getBytes();
	private final byte[] endXMLTag = "}".getBytes();
	private FSDataInputStream fsDataIS;
	private DataOutputBuffer buffer;
	private long start;
	private long end;
	private long pos;
	private Logger logger = Logger.getLogger("project2.inputformat.JSONInputFormat");
	public BKXmlRecordReader(JobConf config, FileSplit split) throws IOException {
		//initialize here
		buffer = new DataOutputBuffer();
		start = split.getStart();
		end = start + split.getLength();
		Path file = split.getPath();
		//open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(config);
		fsDataIS = fs.open(file);
		fsDataIS.seek(start);
		logger.info("start");
	}
	
	public boolean read2MatchStart(byte[] startTagBytes) throws IOException
	{
		int nextByte = -1;
		int currentMatch = 0;
		while ((nextByte = fsDataIS.read()) != -1 && fsDataIS.getPos() < end) {
			//read the next byte.
			if (nextByte == startTagBytes[currentMatch]) {
				//if match, move to next byte
				currentMatch += 1;
				buffer.write(nextByte);
			} else {
				currentMatch = 0;
				buffer.reset();
			}
			if (currentMatch == startTagBytes.length) {
				return true;
			}
			
		}
		return false;
	}
	public boolean read2MatchEnd(byte[] endTagBytes) throws IOException
	{
		int nextByte = -1; int currentMatch = 0;
		while ((nextByte = fsDataIS.read()) != -1 && fsDataIS.getPos() < end) {
//			if (nextByte != endTagBytes[currentMatch]) {
			//save to buffer:
			buffer.write(nextByte);
//			}
			if (nextByte == endTagBytes[currentMatch]) {
				currentMatch += 1;
			} else {
				currentMatch = 0;
			}
			if (currentMatch == endTagBytes.length) {
				return true;
			}
		}
		return false;
	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		fsDataIS.close();
	}

	@Override
	public LongWritable createKey() {
		// TODO Auto-generated method stub
		return new LongWritable();
	}

	@Override
	public Text createValue() {
		// TODO Auto-generated method stub
		return new Text();
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return fsDataIS.getPos();
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return (float)(fsDataIS.getPos() - start) / (float) (end - start);
	}

	@Override
	public boolean next(LongWritable key, Text value) throws IOException {
		// TODO Auto-generated method stub
		try {
			if (fsDataIS.getPos() >= end) return false;
			//read to match the start tag
			if (read2MatchStart(startXMLTag)) {
				if (read2MatchEnd(endXMLTag)) {
					value.set(buffer.getData());
					logger.info(value.toString());
					return true;
				}
			}
		} finally {
			
			buffer.reset();
		}
		return false;
	}
	
}
