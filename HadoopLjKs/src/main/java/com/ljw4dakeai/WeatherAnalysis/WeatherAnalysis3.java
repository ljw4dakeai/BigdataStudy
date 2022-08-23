package com.ljw4dakeai.WeatherAnalysis;

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


public class WeatherAnalysis3 {

    public static String findGrade(float x) {
        String Grade = "";
        if (Math.max(0, x) == Math.min(x, 0.2)) {
            Grade = "无风";
        }
        if (Math.max(0.2, x) == Math.min(x, 3.3)) {
            Grade = "清风";
        }
        if (Math.max(3.3, x) == Math.min(x, 5.4)) {
            Grade = "微风";
        }
        if (Math.max(5.4, x) == Math.min(x, 7.9)) {
            Grade = "和风";
        }
        if (Math.max(7.9, x) == Math.min(x, 13.8)) {
            Grade = "强风";
        }
        if (Math.max(13.8, x) == Math.min(x, 17.1)) {
            Grade = "疾风";
        }
        if (Math.max(17.1, x) == Math.min(x, 21.7)) {
            Grade = "大风";
        }
        if (Math.max(21.7, x) == Math.min(x, 24.1)) {
            Grade = "烈风";
        }
        if (Math.max(24.1, x) == Math.min(x, 28.4)) {
            Grade = "狂风";
        }
        if (Math.max(28.4, x) == Math.min(x, 32.6)) {
            Grade = "暴风";
        }


        return Grade;
    }

    private static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text text = new Text();


        @Override
        public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
            String line = value.toString();
//            01-01	天平均风速是：2.358513
            String[] values = line.split("\\s+");
            String monthDay = values[0];
            float windVelocity = Float.parseFloat(values[1].split("：")[1]);

            text.set(findGrade(windVelocity));
            context.write(text, new IntWritable(1));

        }

    }

    private static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int num = 0;

            for (IntWritable intWritable : values) {
                num += 1;
            }

            context.write(key, new IntWritable(num));

        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(WeatherAnalysis3.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(job, "C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\WeatherAnalysis\\analysis2\\part-r-00000");
        FileOutputFormat.setOutputPath(job, new Path("C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\WeatherAnalysis\\analysis3"));


        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
