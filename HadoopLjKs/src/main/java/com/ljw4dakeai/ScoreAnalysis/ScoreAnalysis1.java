package com.ljw4dakeai.ScoreAnalysis;

import com.ljw4dakeai.pojo.Bean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;


public class ScoreAnalysis1 {

    private static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
        Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\s+");

            String num = values[0];
            String name = values[1];
            int math = Integer.parseInt(values[2]);
            int english = Integer.parseInt(values[3]);

            text.set(num + "-" +  name);

            context.write(text, new FloatWritable((float) math / 2 + (float) english / 2));
        }
    }


    @Test
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(ScoreAnalysis1.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setMapperClass(Map.class);

        FileInputFormat.setInputPaths(job, "com/ljw4dakeai/ScoreAnalysis/score.txt");
        FileOutputFormat.setOutputPath(job, new Path("com/ljw4dakeai/ScoreAnalysis/analysis1"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);




    }

}
