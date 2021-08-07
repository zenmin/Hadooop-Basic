package mr.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * @Describle This Class Is
 * @Author ZengMin
 * @Date 2021/8/7 10:49
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        File file = new File(args[1]);
        if (file.exists()) {
            FileUtil.fullyDelete(file);
        }
        Configuration conf = new Configuration();
        // 创建job对象
        Job job = Job.getInstance(conf);
        // 设置jar存储位置
        job.setJarByClass(WordCountDriver.class);
        // 关联mapper和reduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 设置ampper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 设置最终数据输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置输入路径和输出路径   第一个参数是输入路径  第二个参数是输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        job.submit();
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

}
