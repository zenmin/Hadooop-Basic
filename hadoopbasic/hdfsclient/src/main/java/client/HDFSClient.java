package client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @Describle This Class Is HDFS 基础api
 * @Author ZengMin
 * @Date 2020/2/7 11:03
 */
public class HDFSClient {

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
     * 创建文件夹
     *
     * @return
     */
    @Test
    public void mkdir() {
        Configuration configuration = new Configuration();
        // 此处设置副本数 优先级最高
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI("hdfs://hadoop02:9000"), configuration, "hadoop");
            fileSystem.mkdirs(new Path("/home/zm/dir"));
            fileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试上传
     * <p>
     * hdfs参数优先级  代码 - xml配置 - 服务器namenode配置 - hdfs默认配置
     *
     * @return
     */
    @Test
    public void copyFormLocal() {
        Configuration configuration = new Configuration();
        // 此处设置副本数 优先级最高
        configuration.set("dfs.replication", "2");
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI("hdfs://hadoop02:9000"), configuration, "hadoop");
            fileSystem.copyFromLocalFile(new Path("C:\\Users\\74170\\Desktop\\test.txt"), new Path("/home/zm/test2.txt"));
            fileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 测试下载
     *
     * @return
     */
    @Test
    public void copyToLocal() {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI("hdfs://hadoop02:9000"), configuration, "hadoop");
//            fileSystem.copyToLocalFile(new Path("/home/zm/test2.txt"), new Path("C:\\Users\\74170\\Desktop\\test.txt"));

            // useRawLocalFileSystem为true表示使用用户本地文件系统  将不会产生crc校验文件
            // delsrc=true时  此处下载会删除hdfs上面的文件 而不是本地文件
            fileSystem.copyToLocalFile(false, new Path("/home/zm/test1.txt"),
                    new Path("C:\\Users\\74170\\Desktop\\test2.txt"), true);
            fileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试删除
     */
    @Test
    public void delete() {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI("hdfs://hadoop02:9000"), configuration, "hadoop");
            // b表示是否递归  文件夹必须为true
            fileSystem.delete(new Path("/home/zm2"), true);
            fileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试重命名
     */
    @Test
    public void rename() {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI("hdfs://hadoop02:9000"), configuration, "hadoop");
            fileSystem.rename(new Path("/home/zm/test.txt"), new Path("/home/zm/test1.txt"));
            fileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 文件详情
     */
    @Test
    public void fileDetail() {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI("hdfs://hadoop02:9000"), configuration, "hadoop");

            // 获取所有文件
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/home/zm"), true);
            while (iterator.hasNext()) {
                LocatedFileStatus next = iterator.next();

                // 获取文件详情
                System.out.println("名称：" + next.getPath().getName());
                System.out.println("大小：" + next.getLen());
                System.out.println("权限：" + next.getPermission());
                System.out.println("副本数：" + next.getReplication());
                System.out.println("访问时间：" + next.getAccessTime());
                System.out.println("所有者：" + next.getOwner());

                BlockLocation[] blockLocations = next.getBlockLocations();
                for (BlockLocation blockLocation : blockLocations) {
                    String[] hosts = blockLocation.getHosts();
                    System.out.println("文件存在以下节点：" + Arrays.asList(hosts));
                }

                System.out.println("------------------------------");
            }
            fileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断文件是文件夹还是文件
     */
    @Test
    public void isFile() {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI("hdfs://hadoop02:9000"), configuration, "hadoop");
//            boolean file = fileSystem.isFile(new Path("/home/zm/test.txt"));
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/home/zm"));
            for (FileStatus fileStatus : fileStatuses) {
                System.out.println(fileStatus.getPath().getName() + ": " + (fileStatus.isFile() ? "文件" : "文件夹"));
            }
            fileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
