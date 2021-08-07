package mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Describle This Class Is
 * @Author ZengMin
 * @Date 2021/8/7 9:57
 * 输入数据的key（第几行）  输入数据的value 输出数据key的类型  输出数据value类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text text = new Text();
    IntWritable intWritable = new IntWritable(1);

    /**
     * 读每一行
     * <p>
     * zm  zm  xz
     * xz xz
     * zm
     * <p>
     * write之后变成：{Text: IntWritable}
     * zm:1
     * zm:1
     * xz:1
     * xz:1
     * xz:1
     * zm:1
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取一行
        String line = value.toString();
        // 分隔单词
        String[] word = line.split(" ");
        for (String s : word) {
            text.set(s);
            context.write(text, intWritable);
        }
    }
}

















