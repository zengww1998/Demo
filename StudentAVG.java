package hadoop_homework1;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StudentAVG {
//	Map class 
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split("\\|"); // 0->subject 1->name 2-> score
            // write such as : lisi---->a:58
            context.write(new Text(line[1]), new Text(line[0] + "-" + line[2]));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0; // total score
            int count = 0; // subject numbers
            String str = ""; // the string for recording subject and score
            for (Text value : values) {
            	String[] v = value.toString().split("-"); // get the subject and score
            	str += v[0] + ':' + v[1] + "\t"; // cat all subject and score
                sum += Integer.parseInt(v[1].trim());
                count++;
            }
            String avg = String.format("%.2f", sum*1.0 / count); // format input 
            str += "\tTotal : " + sum; // add the Total score
            str += "\tAVG : " + avg; // add the average score
            context.write(key, new Text(str));
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        String[] otherArgs = new String[]{"homework1/student_avg/", "homework1/student_avg/output"};
        if (otherArgs.length != 2) {
            System.err.println("Usage: AvgScore <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "StudentAVG");
        job.setJarByClass(StudentAVG.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // input path 
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); // output path 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}