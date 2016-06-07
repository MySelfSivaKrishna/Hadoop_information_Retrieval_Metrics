package com.Information_retreival;
// logarithmic term frequency
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;


public class TFIDF extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(com.Information_retreival.TFIDF.class);


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new TFIDF(),args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job1 = Job.getInstance(getConf(),"termfrequency");

        job1.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job1,new Path(args[0]));

        FileOutputFormat.setOutputPath(job1,new Path(args[1]));
        job1.setMapperClass(Map1.class);
        job1.setReducerClass(Reduce1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.submit();
        job1.waitForCompletion(true);


        FileSystem fs=FileSystem.get(job1.getConfiguration());
        FileStatus[] stats = fs.listStatus(new Path(args[0]));

        Configuration config =new Configuration();
        config.set("noInput",Integer.toString(stats.length));// storing no of documents in collection




        Job job2 = Job.getInstance(config,"tfidf");
        job2.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job2,new Path(args[1]));
        FileOutputFormat.setOutputPath(job2,new Path(args[2]));
        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.submit();
        return job2.waitForCompletion(true)?0:1;

    }
    private static class Map1 extends Mapper<LongWritable,Text,Text,IntWritable> {
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
                    i= i+ delimiter + filename+" = ";
                }
                context.write(new Text(i),one);
            }

        }
    }



    private static class Reduce1 extends Reducer<Text,IntWritable,Text,DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for(IntWritable count: values ){
                sum += count.get();


            }
            sum =  1 + Math.log10(sum);
            context.write(key,new DoubleWritable(sum));

        }
    }

    private static class Map2 extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line_input = value.toString();
            String[] output = line_input.split("#####");

            context.write(new Text(output[0]),new Text(output[1]));

        }
    }

    private static class Reduce2 extends Reducer<Text,Text,Text,DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
//
            Double count =0.0d;
            ArrayList<Double>  tf = new ArrayList<>();
            ArrayList<String>  wordswithFilename = new ArrayList<>();
            for(Text file_value:values){
                count +=1.0d;
                tf.add(Double.parseDouble(file_value.toString().split("=")[1]));
                wordswithFilename.add(key.toString()+file_value.toString().split("=")[0]);
            }
            Double totalfiles = Double.parseDouble(config.get("noInput"));
            Double idf =1 + Math.log10(totalfiles/count);
//            // tfidf values in list

            for(int i=0;i<wordswithFilename.size();i++){
                context.write(new Text(wordswithFilename.get(i)),new DoubleWritable(tf.get(i)*idf));
            }
//

        }
    }
}
