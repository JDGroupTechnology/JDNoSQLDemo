import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;


public class HBaseUtil {

    private static Configuration conf = null;
    private static volatile Connection conn = null;

    /**
     * 获取全局唯一的Configuration实例
     *
     * @return
     */
    public static synchronized Configuration getConfiguration() {
        if (conf == null) {
            // 此处从配置文件读取配置信息，配置文件在classpath下的hbase-site.xml。
            conf = HBaseConfiguration.create();
        }
        return conf;
    }

    /**
     * 获取全局唯一的Connection实例
     * Connection对象自带连接池，请使用单例模式获取连接。
     *
     * @return
     * @throws Exception
     */
    public static Connection getConnection()
            throws Exception {
        if (conn == null || conn.isClosed() || conn.isAborted()) {
            synchronized (HBaseUtil.class) {
                if (conn == null || conn.isClosed() || conn.isAborted()) {
                    /*
                     * * 创建一个Connection
                     */
                    //第一种方式：通过配置文件
                    Configuration configuration = getConfiguration();
                    //第二种方式：代码中指定
                    //Configuration configuration = new Configuration();
                    //configuration.set("bdp.hbase.instance.name", "SL1000000003014");//申请的实例名称
                    //configuration.set("bdp.hbase.accesskey", "MZYH5UIKEY3BU7CNB5FWLS2OTA");//实例对应的accesskey，请妥善保管你的AccessKey
                    conn = ConnectionFactory.createConnection(configuration);
                }
            }
        }
        return conn;
    }

    /**
     * Create a new Connection instance using the passed conf instance.
     *
     * 请使用getConnection()方式获取连接，该方法在多实例情况下使用
     * @return  Connection object for conf
     * @throws IOException
     */
    public static Connection createConnection()
            throws IOException {
        Configuration conf = new Configuration();
        conf.set("bdp.hbase.instance.name", "SL1000000003014");
        conf.set("bdp.hbase.accesskey", "MZYH5UIKEY3BU7CNB5FWLS2OTA");
        conf.set("hbase.client.write.buffer", "20971520");
        return ConnectionFactory.createConnection(conf);
    }
}
