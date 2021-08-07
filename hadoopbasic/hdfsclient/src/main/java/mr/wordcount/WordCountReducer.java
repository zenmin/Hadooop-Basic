package mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Describle This Class Is
 * @Author ZengMin
 * @Date 2021/8/7 10:10
 * <p>
 * KEYIN,VALUEIN,  Map阶段输入的kv
 * KEYOUT,VALUEOUT
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable intWritable = new IntWritable();

    /**
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        // 累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }
        intWritable.set(sum);
        // 输出
        context.write(key, intWritable);
    }
}
