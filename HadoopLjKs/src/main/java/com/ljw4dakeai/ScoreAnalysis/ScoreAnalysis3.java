package com.ljw4dakeai.ScoreAnalysis;


import com.ljw4dakeai.pojo.Bean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author zoujiahao
 */
public class ScoreAnalysis3 {


    private static class Reduce extends Reducer<IntWritable, Bean, Text, NullWritable> {
        Text text = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Bean> values, Context context) throws IOException, InterruptedException {
            int mathMax = 0;
            int mathMin = 100;
            int englishMax = 0;
            int englishMin = 100;

            for (Bean bean : values) {
                if (mathMax < bean.getMath()){
                    mathMax = bean.getMath();
                }
                if (mathMin > bean.getMath()){
                    mathMin = bean.getMath();
                }
                if (englishMax < bean.getEnglish()){
                    englishMax = bean.getEnglish();
                }
                if (englishMin > bean.getEnglish()){
                    englishMin = bean.getEnglish();
                }

            }

            String output = "math的最高成绩为：" + (mathMax)  + "，最低成绩为：" + (mathMin)+  ";" + "english的最高成绩为：" + (englishMax)+ "，最低成绩为：" + (englishMin);
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
        job.setMapperClass(ScoreAnalysis2.Map.class);
        job.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(job, "C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\ScoreAnalysis\\score.txt");
        FileOutputFormat.setOutputPath(job, new Path("C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\ScoreAnalysis\\analysis3"));


        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }

}
