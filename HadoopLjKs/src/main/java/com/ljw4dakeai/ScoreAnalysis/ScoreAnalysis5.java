package com.ljw4dakeai.ScoreAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


import static com.ljw4dakeai.ScoreAnalysis.ScoreAnalysis4.findGrade;

/**
 * @author zoujiahao
 */
public class ScoreAnalysis5 {
    private static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\s+");

            String num = values[0];
            String name = values[1];
            int math = Integer.parseInt(values[2]);
            int english = Integer.parseInt(values[3]);


            text.set("english" + findGrade(english));


            context.write(text , new IntWritable(1));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(ScoreAnalysis5.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(ScoreAnalysis4.Reduce.class);

        FileInputFormat.setInputPaths(job, "C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\ScoreAnalysis\\score.txt");
        FileOutputFormat.setOutputPath(job, new Path("C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\ScoreAnalysis\\analysis5"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
