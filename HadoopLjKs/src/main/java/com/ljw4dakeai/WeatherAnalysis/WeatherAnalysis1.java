package com.ljw4dakeai.WeatherAnalysis;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



public class WeatherAnalysis1 {

    static class Bean implements Writable {
        private String month;
        private String day;
        private Float temperature;
        private Float windVelocity;

        public Bean() {
            super();
        }

        public Bean(String month, String day, Float temperature, Float windVelocity) {
            this.month = month;
            this.day = day;
            this.temperature = temperature;
            this.windVelocity = windVelocity;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(month);
            dataOutput.writeUTF(day);
            dataOutput.writeFloat(temperature);
            dataOutput.writeFloat(windVelocity);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            month = dataInput.readUTF();
            day = dataInput.readUTF();
            temperature = dataInput.readFloat();
            windVelocity = dataInput.readFloat();

        }

        public String getMonth() {
            return month;
        }

        public void setMonth(String month) {
            this.month = month;
        }

        public String getDay() {
            return day;
        }

        public void setDay(String day) {
            this.day = day;
        }

        public Float getTemperature() {
            return temperature;
        }

        public void setTemperature(Float temperature) {
            this.temperature = temperature;
        }

        public Float getWindVelocity() {
            return windVelocity;
        }

        public void setWindVelocity(Float windVelocity) {
            this.windVelocity = windVelocity;
        }
    }

    private static class Map extends Mapper<LongWritable, Text, Text, Bean> {
        Bean bean = new Bean();
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
            float windVelocity = Float.parseFloat(values[8]);

            text.set(month);
            bean.setMonth(month);
            bean.setDay(day);
            bean.setTemperature(temperature);
            bean.setWindVelocity(windVelocity);

            context.write(text, bean);

        }

    }

    public static class Reduce extends Reducer<Text, Bean, Text, Text> {
        Text text = new Text();

        @Override
        protected void reduce(Text key, Iterable<Bean> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            float temperatureAll = 0;
            for (Bean bean : values) {
                temperatureAll += bean.getTemperature();
                num += 1;
            }

            String output = "月平均温度是：" + temperatureAll / (float)num;
            text.set(output);

            context.write(key, text);

        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(WeatherAnalysis1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Bean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);


        FileInputFormat.setInputPaths(job, "C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\WeatherAnalysis\\china_isd_lite_2021\\*\\");
        FileOutputFormat.setOutputPath(job, new Path("C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\WeatherAnalysis\\analysis1"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }


}
