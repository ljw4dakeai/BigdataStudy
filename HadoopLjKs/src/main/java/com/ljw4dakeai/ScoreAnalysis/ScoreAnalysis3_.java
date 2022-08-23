package com.ljw4dakeai.ScoreAnalysis;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class ScoreAnalysis3_ {


    private static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        Text text = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int maxScore = 0;
            int minScore = 100;

            for (IntWritable value : values) {
                if (maxScore < value.get()){
                    maxScore = value.get();
                }
                if (minScore > value.get()){
                    minScore =  value.get();
                }

            }

            String output = "的最高成绩为：" + (maxScore)  + "，最低成绩为：" + (minScore);
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
        job.setMapperClass(ScoreAnalysis2_.Map.class);
        job.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(job, "C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\ScoreAnalysis\\score.txt");
        FileOutputFormat.setOutputPath(job, new Path("C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\ScoreAnalysis\\analysis3_"));


        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }

}
