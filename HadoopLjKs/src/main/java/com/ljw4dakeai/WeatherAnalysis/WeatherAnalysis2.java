package com.ljw4dakeai.WeatherAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WeatherAnalysis2 {

    private static class Map extends Mapper<LongWritable, Text, Text, WeatherAnalysis1.Bean> {
        WeatherAnalysis1.Bean bean = new WeatherAnalysis1.Bean();
        Text text = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] values = line.split("\\s+");
            if ("-9999".equals(values[4])) {
                return;
            }
            if ("-9999".equals(values[5])) {
                return;
            }
            if ("-9999".equals(values[6])) {
                return;
            }
            if ("-9999".equals(values[7])) {
                return;
            }
            if ("-9999".equals(values[8])) {
                return;
            }
            if (values.length != 12) {
                return;
            }
//        2021 01 01 00    80   -94 10285    50    60     1 -9999 -9999
//        2021 01  01  00   80        -94            10285           50    60         1                      -9999                                 -9999
//        年  月 日 小时 气温(/10) 露点温度(/10) 海平面气压(/10) 风向 风速(/10)  天空条件总覆盖代码 液体沉淀深度维度-一小时间隔(/10) 液体沉淀深度维度-六小时间隔(/10)

            String month = values[1];
            String day = values[2];
            float temperature = Float.parseFloat(values[4]) / (float)10 ;
            float windVelocity = Float.parseFloat(values[8]) / (float)10;

            text.set(month + "-" +  day);
            bean.setMonth(month);
            bean.setDay(day);
            bean.setTemperature(temperature);
            bean.setWindVelocity(windVelocity);

            context.write(text, bean);

        }

    }

    public static class Reduce extends Reducer<Text, WeatherAnalysis1.Bean, Text, Text> {
        Text text = new Text();

        @Override
        protected void reduce(Text key, Iterable<WeatherAnalysis1.Bean> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            float windVelocityAll = 0;
            for (WeatherAnalysis1.Bean bean : values) {
                windVelocityAll += bean.getWindVelocity();
                num += 1;
            }

            String output = "天平均风速是：" + windVelocityAll / (float)num;
            text.set(output);

            context.write(key, text);

        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(WeatherAnalysis2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WeatherAnalysis1.Bean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(WeatherAnalysis2.Map.class);
        job.setReducerClass(WeatherAnalysis2.Reduce.class);


        FileInputFormat.setInputPaths(job, "C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\WeatherAnalysis\\china_isd_lite_2021\\*\\");
        FileOutputFormat.setOutputPath(job, new Path("C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\WeatherAnalysis\\analysis2"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
