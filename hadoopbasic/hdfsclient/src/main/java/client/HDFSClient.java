package client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Describle This Class Is
 * @Author ZengMin
 * @Date 2020/2/7 11:03
 */
public class HDFSClient {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        // 设置配置信息
        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS","hdfs://hadoop02:9000");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop02:9000"), configuration, "hadoop");

        // 操作hdfs
        boolean mkdirs = fileSystem.mkdirs(new Path("/home/zm2"));

        // 关闭连接
        fileSystem.close();

        System.out.println(mkdirs);

    }


}
