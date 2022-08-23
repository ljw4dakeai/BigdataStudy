package com.ljw4dakeai.ScoreAnalysis;

import  com.ljw4dakeai.pojo.Bean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author zoujiahao
 */
public class ScoreAnalysis2 {

    static class Map extends Mapper<LongWritable, Text, IntWritable, Bean> {
        Bean bean = new Bean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\s+");

            String num = values[0];
            String name = values[1];
            int math = Integer.parseInt(values[2]);
            int english = Integer.parseInt(values[3]);


            bean.setNum(num);
            bean.setName(name);
            bean.setMath(math);
            bean.setEnglish(english);

            context.write(new IntWritable(1), bean);
        }
    }

    private static class Reduce extends Reducer<IntWritable, Bean, Text, NullWritable> {
        Text text = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Bean> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            int math = 0;
            int english = 0;

            for (Bean bean : values) {
                num += 1;
                math += bean.getMath();
                english += bean.getEnglish();
            }

            String output = "math的平均成绩为：" + (math / num) + ";" + "english的平均成绩为：" + (english / num);
            text.set(output);

            context.write(text, NullWritable.get());
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(ScoreAnalysis2.class);


        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Bean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(job, "C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\ScoreAnalysis\\score.txt");
        FileOutputFormat.setOutputPath(job, new Path("C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\ScoreAnalysis\\analysis2"));


        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }
}
