import java.io.IOException;
import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class Problem1_1 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {
        NavigableMap<Integer, String> map;
        String W;
        protected void setup(Context context){
            // key is input x. value is output range ID
            Configuration conf = context.getConfiguration();
            W = conf.get("W");
            map = new TreeMap<Integer, String>();
            map.put(0, "A"); // 0 ... 1000 -> A
            map.put(1001, "B");// 1001 ... 2000 ->B
            map.put(2001, "C");
            map.put(3001, "D");
            map.put(4001, "E");
            map.put(5001, "F");
            map.put(6001, "G");
            map.put(7001, "H");
            map.put(8001, "I");
            map.put(9001, "J");
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // mapper partition the space based on range
            // if left_bottom_point is located in this range, then we could put it on this range

            System.out.println(W);
            String filePath = ((FileSplit)context.getInputSplit()).getPath().toString();
            if (filePath.contains("Rectangles")) {
                String[] line = value.toString().split(","); // 按分隔符分割出字段
                String recID = "r"+line[0] + ",";
//                String marker = "@";
                int leftBottomX = Integer.parseInt(line[1]);
                int leftBottomY = Integer.parseInt(line[2]);
                int rightTopX = Integer.parseInt(line[3]);
                int rightTopY = Integer.parseInt(line[4]);
                String outPutKey1 = "-1";
                String outPutKey2 = "-2";
                if (W == null){
                    outPutKey1 = map.floorEntry(leftBottomX).getValue();
                    outPutKey2 = map.floorEntry(rightTopX).getValue();
                    if (outPutKey1 == outPutKey2){
                        context.write(new Text(outPutKey1), new Text(recID+ leftBottomX + "," + leftBottomY + ","
                                + rightTopX + "," + rightTopY + ","));
                    }
                    else {
                        context.write(new Text(outPutKey1), new Text(recID+ leftBottomX + "," + leftBottomY + ","
                                + rightTopX + "," + rightTopY + ","));
                        context.write(new Text(outPutKey2), new Text(recID+ leftBottomX + "," + leftBottomY + ","
                                + rightTopX + "," + rightTopY + ","));
                    }
                } else {
                    String[]temp = W.split(",");
                    int wLeftX = Integer.parseInt(temp[0]);
                    int wLeftY = Integer.parseInt(temp[1]);
                    int wRightX = Integer.parseInt(temp[2]);
                    int wRightY = Integer.parseInt(temp[3]);
                    if (leftBottomX >= wLeftX && leftBottomX <= wRightX &&
                            wLeftY <= leftBottomY && leftBottomY <= wRightY){
                        outPutKey1 = "W";
                    }
                    if (rightTopX >= wLeftX && rightTopX <= wRightX &&
                            rightTopY >= wLeftY && rightTopY <= wRightY){
                        outPutKey2 = "W";
                    }
                    if (outPutKey1 == "W" || outPutKey2 == "W"){
                        context.write(new Text("W"), new Text(recID+ leftBottomX + "," + leftBottomY + ","
                                + rightTopX + "," + rightTopY + ","));
                    }
                }


            }
            if (filePath.contains("Points")){
                String[] line = value.toString().split(",");
                String pid = line[0];
//                String marker = "@";
                int x = Integer.parseInt(line[1]);
                int y = Integer.parseInt(line[2]);
                if (W == null){
                    String outPutKey = map.floorEntry(x).getValue();
                    context.write(new Text(outPutKey), new Text( x + "," + y + ","));
                } else {
                    String[]temp = W.split(",");
                    int wLeftX = Integer.parseInt(temp[0]);
                    int wLeftY = Integer.parseInt(temp[1]);
                    int wRightX = Integer.parseInt(temp[2]);
                    int wRightY = Integer.parseInt(temp[3]);
                    if (x >= wLeftX && x <= wRightX && y >= wLeftY && y <= wRightY){
                        context.write(new Text("W"), new Text( x + "," + y + ","));
                    }
                }
            }
        }
    }
    public static class RangeReducer
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            ArrayList<String> recs = new ArrayList<String>();
            ArrayList<String> points = new ArrayList<String>();
            for (Text val : values){
//                System.out.println(val.toString());
                String[] line = val.toString().trim().split("//s+");
                for (String ele : line){
                    // this is a rectangle
//                    System.out.println(ele);
                    if (ele.isEmpty())continue;

                    if (ele.contains("r")){
                        recs.add(ele);
                    } else {
//                        System.out.println(ele);
                        points.add(ele);
                    }
                }
            }
            for (String rec : recs){
                String[] line = rec.split(",");

                int leftBottomX = Integer.parseInt(line[1]);
                int leftBottomY = Integer.parseInt(line[2]);
                int rightTopX = Integer.parseInt(line[3]);
                int rightTopY = Integer.parseInt(line[4]);
                String recId = line[0];
                for (String point : points){
//                    System.out.println("point is" + point);
                    String[] tempP = point.split(",");
//                    for (String e: tempP){
//                        System.out.println(e);
//                    }
                    int x = Integer.parseInt(tempP[0]);
                    int y = Integer.parseInt(tempP[1]);
                    if (x >= leftBottomX && x <= rightTopX && y >= leftBottomY && y <=rightTopY){
                        context.write(new Text(recId), new Text(x + "," + y));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // when user input W, implemented a naive version
        String W = "";
        String inputP = "/home/supper/IdeaProjects/pj2/src/input/Points_sanity_check.txt";
        String inputR = "/home/supper/IdeaProjects/pj2/src/input/Rectangles_sanity_check.txt";
        String outputP1 = "/home/supper/IdeaProjects/pj2/src/outputSanityP1_no_W/";
        Configuration conf = new Configuration();
        conf.set("W", W);
        Job job = new Job(conf, "Problem1_1");
        job.setJarByClass(Problem1_1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(RangeReducer.class);
        job.setOutputKeyClass(Text.class);
//        DistributedCache.addCacheFile(new URI("/home/supper/IdeaProjects/pj2/src/input/Points.txt"), job.getConfiguration());
        job.setNumReduceTasks(10);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputR));
        FileInputFormat.addInputPath(job, new Path(inputP));
        FileOutputFormat.setOutputPath(job, new Path(outputP1));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}