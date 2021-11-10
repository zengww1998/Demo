package hadoop_homework1;


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class InverseIndex {
	public static  HashSet<String> fileNames = new HashSet<>(); // record all filenames 

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		@Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        if (value != null){
	            String line = value.toString();
	            String[] words = line.split(" "); // split words with space
	            if (words.length > 0){
	                FileSplit fileSplit = (FileSplit)context.getInputSplit(); // get file information
	                String fileName = fileSplit.getPath().getName(); // get filename
	                
	                InverseIndex.fileNames.add(fileName); // record filename
	                
	                for (String word : words) {
	                    context.write(new Text(word),new Text(new LongWritable(1) + "-" + fileName));
	                }
	            }
	        }
		}
	}
	
	public static class Reduce extends Reducer<Text, Text,Text,Text>{
		@Override
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        if (values != null){
	            String str = ""; // return string
	            HashMap<String, Integer> record = new HashMap<String, Integer>(); // record every word emerge in file
	            for (Text value : values) {
	                String filename = value.toString().split("-")[1];
	                int times = Integer.parseInt(value.toString().split("-")[0]); // word appear in current file times

	                if(null == record.get(filename)) {
	                	record.put(filename, times);
	                }else {
	                	int time = (int) record.get(filename);
	                	time += 1;
	                	record.put(filename, time);
	                }
	            }
	            for(String fileName:InverseIndex.fileNames) {
	            	str += fileName;
	            	if(null == record.get(fileName)) {
	            		str += " : 0";
	            	}else{
	            		str += " : " + record.get(fileName);
	            	}
	            	str += '\t';
	            }
	            context.write(key, new Text(str));
	        }
	    }
	}
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        String[] otherArgs = new String[]{"homework1/inverse_index/", "homework1/inverse_index/output"};
        if (otherArgs.length != 2) {
            System.err.println("Usage: AvgScore <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "InverseIndex");
        job.setJarByClass(InverseIndex.class);
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
