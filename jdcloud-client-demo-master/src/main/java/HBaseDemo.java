import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 使用HBase的基本Java客户端访问HBase。
 * 该工程经测试通过。
 * 测试表为hbase_test:tb_user，表参数为：{NAME => 'hbase_test:tb_user', FAMILIES => [{NAME => 'info'}]}
 *
 *
 * 创建HBase连接对象请务必保持单例模式，HBase连接对象自带连接池，单例模式创建连接参考HBaseUtil。
 * HBase连接对象connection创建会比较耗时，在使用过程中，尽量全局复用连接对象，勿多次创建和关闭。
 */
public class HBaseDemo {
    private static String nameSpace = "test_namespace123";
    private static String table = "test_table";
    private static String columnFamily = "d";

    /**
     * 同步写
     *
     * @param tableSpace      表空间
     * @param tableName       表名
     * @param rowkey          行键(此处测试使用了string类型，实际可以任意类型拼接的byte[])
     * @param columnFamily    列族
     * @param columnQualifier 列名
     * @param value           值
     * @return boolean 是否插入成功
     * @throws Exception
     */
    public Boolean put(String tableSpace, String tableName, String rowkey, String columnFamily, String columnQualifier, String value) throws Exception {
        Table table = null;
        Connection connection = null;
        boolean flag = false;
        try {
            connection = HBaseUtil.getConnection();
            table = connection.getTable(TableName.valueOf(tableSpace + ":" + tableName));
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier), Bytes.toBytes(value));
            table.put(put);
            flag = true;
        } finally {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                //如果频繁使用，不用关闭连接
                connection.close();
            }
        }
        return flag;
    }

    // 异步写
    public void testNonsyncPut() throws Exception {
        BufferedMutator bufferedMutator = null;
        Connection connection = null;
        try {
            connection = HBaseUtil.getConnection();
            bufferedMutator = connection.getBufferedMutator(TableName.valueOf("hbase_test:tb_user"));
            Put put = new Put(Bytes.toBytes("user001"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("张三"));
            bufferedMutator.mutate(put);
            List<Put> putList = new LinkedList<Put>();
            Put put1 = new Put(Bytes.toBytes("user002"));
            put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("李四"));
            Put put2 = new Put(Bytes.toBytes("user003"));
            put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("王五"));
            putList.add(put1);
            putList.add(put2);
            bufferedMutator.mutate(putList);//批量异步写

        } finally {
            if (bufferedMutator != null) {
                bufferedMutator.close();
            }
            if (connection != null) {
                //如果频繁使用，不用关闭连接
                connection.close();
            }
        }

    }

    /**
     * @param tableSpace 表空间
     * @param tableName  表名
     * @param rowkey     行键(此处测试使用了string类型，实际可以任意类型拼接的byte[])
     */
    public static void get(String tableSpace, String tableName, String rowkey) throws Exception {
        Table table = null;
        Connection connection = null;
        try {
            connection = HBaseUtil.getConnection();
            table = connection.getTable(TableName.valueOf(tableSpace + ":" + tableName));
            Get get = new Get(Bytes.toBytes(rowkey));
            //如果列数较多，可以指定拿特定列的值
            get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
            Result result = table.get(get);
            for (Cell cell : result.listCells()) {
                System.out.println("qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("value:" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("-------------------------------");
            }
        } finally {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                //如果频繁使用，不用关闭连接
                connection.close();
            }
        }
    }

    /**
     * 通过getList获取多条记录
     *
     * @param tableSpace 表空间
     * @param tableName  表名
     * @param getList    多个自定义的Get
     * @return 结果集
     * @throws Exception
     */
    public List<Map<String, Object>> getByGetList(String tableSpace, String tableName, List<Get> getList) throws Exception {
        Table table = null;
        Connection connection = null;
        List<Map<String, Object>> getResults;
        try {
            connection = HBaseUtil.getConnection();
            table = connection.getTable(TableName.valueOf(tableSpace + ":" + tableName));
            Result[] results = table.get(getList);
            getResults = new LinkedList<Map<String, Object>>();
            for (Result result : results) {
                List<Cell> ceList = result.listCells();
                Map<String, Object> map = new HashMap<String, Object>();
                if (ceList != null && ceList.size() > 0) {
                    for (Cell cell : ceList) {
                        map.put(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) +
                                        "_" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    }
                }
                getResults.add(map);
            }
        } finally {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                //如果频繁使用，不用关闭连接
                connection.close();
            }
        }
        return getResults;
    }



    public void testPut() throws Exception {
        put("hbase_test", "tb_user", "user001", "info", "name", "wangwu");
    }



    public void testGet() throws Exception {
        get("hbase_test", "tb_user", "user001");
    }


    public void testGetByGetList() {
        List<Get> getList = new ArrayList<Get>();
        Get get1 = new Get(Bytes.toBytes("user001"));
        Get get2 = new Get(Bytes.toBytes("user002"));
        getList.add(get1);
        getList.add(get2);
        try {
            List<Map<String, Object>> users = getByGetList("hbase_test", "tb_user", getList);
            for (Map<String, Object> user : users) {
                Iterator<Map.Entry<String, Object>> entries = user.entrySet().iterator();
                while (entries.hasNext()) {
                    Map.Entry<String, Object> entry = entries.next();
                    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
                }
                System.out.println("-------------------------------");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void testScan() throws Exception {
        Connection connection = null;
        Table table = null;
        ResultScanner scanner = null;
        try {
            connection = HBaseUtil.getConnection();
            table = connection.getTable(TableName.valueOf("hbase_test:tb_user"));
            Scan scan = new Scan();
            //请根据业务场景合理设置StartKey，StopKey
            scan.setStartRow(Bytes.toBytes("000"));
            scan.setStopRow(Bytes.toBytes("user01"));
            scanner = table.getScanner(scan);
            for (Result result : scanner) {
                System.out.println(Bytes.toString(result.getRow()));
                for (Cell cell : result.listCells()) {
                    System.out.println("qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println("value:" + Bytes.toString(CellUtil.cloneValue(cell)));
                    System.out.println("**************");
                }
                System.out.println("-------------------------------");
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                //如果频繁使用，不用关闭连接
                connection.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        HBaseDemo hBaseDemo = new HBaseDemo();
        hBaseDemo.put(nameSpace, table, "user001", columnFamily, "name", "wangwu");
        hBaseDemo.put(nameSpace, table, "user002", columnFamily, "name", "王五");
        get(nameSpace, table, "user001");
        get(nameSpace, table, "user002");

    }
}
