package com.Information_retreival;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class DocWordCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new com.Information_retreival.DocWordCount(),args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(),"docwordcount");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    private static class Map extends Mapper<LongWritable,Text,Text,IntWritable> {
        IntWritable one = new IntWritable(1);



        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] text= value.toString().split(" ");
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String delimiter = "#####";
            for(String i:text){
                if(i.isEmpty()){
                    continue;
                }
                else {
                    i= i+ delimiter + filename;
                }
                context.write(new Text(i),one);
            }

        }
    }



    private static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable count: values ){
                sum += count.get();


            }
            context.write(key,new IntWritable(sum));

        }
    }
}
