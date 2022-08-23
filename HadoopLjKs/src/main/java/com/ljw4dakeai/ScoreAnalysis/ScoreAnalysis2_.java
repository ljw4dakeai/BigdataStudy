package com.ljw4dakeai.ScoreAnalysis;

import com.ljw4dakeai.pojo.Score;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;


public class ScoreAnalysis2_ {

    static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\s+");
            Score math = new Score("math", Integer.parseInt(values[2]));
            Score english = new Score("english", Integer.parseInt(values[3]));
            Score[] scores = new Score[]{math, english};

            for (Score score : scores){
                context.write(new Text(score.getScoreName()), new IntWritable(score.getScore()));
            }

        }
    }

    private static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        Text text = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            int scoreAll = 0;

            for (IntWritable value : values) {
                num += 1;
                scoreAll += value.get();
            }

            String output = "的平均成绩为：" + (scoreAll / num);
            text.set(output);

            context.write(key, text);
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(ScoreAnalysis2.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(job, "C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\ScoreAnalysis\\score.txt");
        FileOutputFormat.setOutputPath(job, new Path("C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\ScoreAnalysis\\analysis2_"));


        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }
}

