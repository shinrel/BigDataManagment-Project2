import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Problem2 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // mapper partition the space based on range
            // if left_bottom_point is located in this range, then we could put it on this range
            String line = value.toString();
            String elevation = line.split(",")[8].trim();
            String eleKey = elevation.split(":")[1].trim();
            context.write(new Text(eleKey), new Text("1"));
        }
    }
    public static class SumReducer
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values){
                sum += Integer.parseInt(val.toString());
            }
            context.write(key, new Text(Integer.toString(sum)));
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "/home/supper/IdeaProjects/pj2/src/input/project2-airfield-dataset.txt";
        String outputP2 = "/home/supper/IdeaProjects/pj2/src/outputSanityP2/";
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
//        test splits correction
//        Path path = new Path(input);
//        long length = hdfs.getLength(path);
//        conf.setLong("mapred.max.split.size", length / 5);

        Job job = new Job(conf, "Problem2");
        job.setInputFormatClass(JsonInputFormat.class);
        job.setJarByClass(Problem2.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outputP2));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}