package client;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @Describle This Class Is HDFS io 操作
 * @Author ZengMin
 * @Date 2020/2/7 11:03
 */
public class HDFSIO {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        // 设置配置信息
        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS","hdfs://hadoop02:9000");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop02:9000"), configuration, "hadoop");

        // 测试连通性
        String scheme = fileSystem.getCanonicalServiceName();

        // 关闭连接
        fileSystem.close();

        System.out.println(scheme);

    }

    /**
     * 流上传 create
     *
     * @throws Exception
     */
    @Test
    public void testUpload() throws Exception {
        // 获取对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop02:9000"), new Configuration(), "hadoop");
        // 获取输入流
        FileInputStream fileInputStream = new FileInputStream(new File("D:\\我的文档\\Java\\my.ini"));
        // 获取输出流
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/home/zm/my.ini"));
        // 流的对拷
        IOUtils.copy(fileInputStream, fsDataOutputStream);
        // 关闭资源
        fsDataOutputStream.close();
        fileInputStream.close();
    }

    /**
     * 流下载 open
     *
     * @throws Exception
     */
    @Test
    public void testDownload() throws Exception {
        // 获取对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop02:9000"), new Configuration(), "hadoop");
        // 获取输入流
        FSDataInputStream in = fileSystem.open(new Path("/home/zm/my.ini"));
        // 获取输出流
        FileOutputStream out = new FileOutputStream(new File("C:\\Users\\74170\\Desktop\\my.ini"));
        // 流的对拷
        IOUtils.copy(in, out);
        // 关闭资源
        out.close();
        in.close();
    }

    /**
     * 流下载 open 只下载第一块  原生方式
     *
     * @throws Exception
     */
    @Test
    public void testDownloadByBlock() throws Exception {
        // 获取对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop02:9000"), new Configuration(), "hadoop");
        // 获取输入流
        FSDataInputStream in = fileSystem.open(new Path("/home/zm/my.ini"));
        // 获取输出流
        FileOutputStream out = new FileOutputStream(new File("C:\\Users\\74170\\Desktop\\my.ini"));
        // 流的对拷  只拷贝指定大小
        byte[] b = new byte[256];
        for (int i = 0; i < 256 * 4; i++) { // 只拷贝了1024kb
            in.read(b);
            out.write(b);
        }
        // 关闭资源
        out.close();
//        in.close();
        org.apache.hadoop.io.IOUtils.closeStream(in);

    }

    /**
     * 流下载 open 只下载第一块  HDFS方式
     *
     * @throws Exception
     */
    @Test
    public void testDownloadByBlockTwo() throws Exception {
        // 获取对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop02:9000"), new Configuration(), "hadoop");
        // 获取输入流
        FSDataInputStream in = fileSystem.open(new Path("/home/zm/LICENSE.txt"));
        // 获取输出流
        FileOutputStream out = new FileOutputStream(new File("C:\\Users\\74170\\Desktop\\LICENSE.txt"));

        // 设置读取起点
        in.seek(1024 * 10);    // 从10KB开始
        // 流的对拷  只拷贝指定大小
        IOUtils.copy(in, out);
        // 关闭资源
        out.close();
//        in.close();
        org.apache.hadoop.io.IOUtils.closeStream(in);

    }

}
