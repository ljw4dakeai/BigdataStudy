package com.ljw4dakeai.ScoreAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class ScoreAnalysis4_ {
    public static String findGrade(int score) {
        String grade = "";
        if (Math.max(0, score) == Math.min(score, 60)) {
            grade = "不及格";
        }
        if (Math.max(60, score) == Math.min(score, 80)) {
            grade = "一般";
        }
        if (Math.max(80, score) == Math.min(score, 90)) {
            grade = "良好";
        }
        if (Math.max(90, score) == Math.min(score, 100)) {
            grade = "优秀";
        }


        return grade;

    }

    private static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\s+");


            int math = Integer.parseInt(values[2]);
            int english = Integer.parseInt(values[3]);


            context.write(new Text("math"  + findGrade(math)) , new IntWritable(1));
            context.write(new Text("english"  + findGrade(english)) , new IntWritable(1));
            context.write(new Text("总人数"), new IntWritable(1));
        }
    }

    static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable intWritable = new IntWritable();

        @Override
        public void reduce(Text	key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for (IntWritable intWritable : values ){
                sum += intWritable.get();
            }
            intWritable.set(sum);
            context.write(key, intWritable);
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(ScoreAnalysis4.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(job, "C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\ScoreAnalysis\\score.txt");
        FileOutputFormat.setOutputPath(job, new Path("C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\ScoreAnalysis\\analysis4_"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }


}
