package project2.inputformat;

import java.io.IOException;
import java.util.logging.Logger;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.codehaus.jackson.map.ObjectMapper;

import project2.obj.Problem2JsonObj;

public class JSONRecordReader2 extends RecordReader<LongWritable, Text> {
	private final byte[] startXMLTag = "{".getBytes();
	private final byte[] endXMLTag = "}".getBytes();
	private FSDataInputStream fsDataIS;
	private DataOutputBuffer buffer;
	private Text value;
	private LongWritable key;
	private long start;
	private long end;
	private long pos;
	private int maxLineLength;
	private LineReader in;
	private CompressionCodecFactory compressionCodecs = null;

	private static final Log LOG = LogFactory.getLog(JSONRecordReader2.class
			.getName());

	public JSONRecordReader2() {

	}

	public boolean read2MatchStart(byte[] startTagBytes) throws IOException {
		int nextByte = -1;
		int currentMatch = 0;
		while ((nextByte = fsDataIS.read()) != -1 && fsDataIS.getPos() < end) {
			// read the next byte.
			if (nextByte == startTagBytes[currentMatch]) {
				// if match, move to next byte
				currentMatch += 1;
				// buffer.write(nextByte);
			} else {
				currentMatch = 0;
				// buffer.reset();
			}
			if (currentMatch == startTagBytes.length) {
				return true;
			}

		}
		return false;
	}

	public boolean read2MatchEnd(byte[] endTagBytes) throws IOException {
		int nextByte = -1;
		int currentMatch = 0;
		while ((nextByte = fsDataIS.read()) != -1 
				&& fsDataIS.getPos() < end) {
			// if (nextByte != endTagBytes[currentMatch]) {
			// save to buffer:
			buffer.write(nextByte);
			// }
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
	public synchronized void close() throws IOException {
		// TODO Auto-generated method stub
		fsDataIS.close();
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return (float) (fsDataIS.getPos() - start) / (float) (end - start);
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		// TODO Auto-generated method stub
		try {
			if (key == null) {
				key = new LongWritable();
			}
			key.set(pos);
			if (value == null) {
				value = new Text();
			}
			if (fsDataIS.getPos() >= end) {
				LOG.info("May LOST THE OBJECT: " + new String(buffer.getData()));
				return false;
			}
			// read to match the start tag
			if (read2MatchStart(startXMLTag)) {
				buffer.write(startXMLTag);
				if (read2MatchEnd(endXMLTag)) {
					// buffer.write(endXMLTag);
					byte[] data = buffer.getData();
					String jsonStr = new String(data);
					// LOG.info(jsonStr);
					// convert to json object:
					Problem2JsonObj jsonObj = Problem2JsonObj.parse(jsonStr);
					value.set(jsonObj.toString());
					// value.set(jsonStr);
					pos += buffer.size();
					buffer = new DataOutputBuffer();
					return true;
				}
			}
		} finally {

			buffer.reset();
		}
		return false;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// initialize here
		 FileSplit split = (FileSplit) genericSplit;
		 Configuration config = context.getConfiguration();
		 buffer = new DataOutputBuffer();
		 start = split.getStart();
		 end = start + split.getLength();
		 Path file = split.getPath();
		 // open the file and seek to the start of the split
		 FileSystem fs = file.getFileSystem(config);
		 fsDataIS = fs.open(file);
		
		 // LOG.info("start .." + start);
		 // boolean skipFirstLine = false;
		 // if (start != 0) {
		 // skipFirstLine = true;
		 // /// Set the file pointer at "start - 1" position.
		 // // This is to make sure we won't miss any jsonObj
		 // --start;
		 // }
		 fsDataIS.seek(start);
		 pos = start;

		
	}

}
