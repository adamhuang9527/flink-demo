package streaming.tablefunction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;


/**
 * CREATE TABLE sideTable(
 * rowkey varchar,
 * `cf.name` varchar,
 * `cf.info` int,
 * PRIMARY KEY(rowkey),
 * PERIOD FOR SYSTEM_TIME
 * )WITH(
 * type ='hbase',
 * zookeeperQuorum ='rdos1:2181',
 * tableName ='workerinfo',
 * cache ='LRU',
 * cacheSize ='10000',
 * cacheTTLMs ='60000',
 * parallelism ='1',
 * partitionedJoin='true'
 * );
 */
public class HbaseTest {
    Configuration conf = null;
    Connection connection = null;

    @Before
    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.160.85.185");
        connection = ConnectionFactory.createConnection(conf);
    }


    @Test
    public void testListTables() throws IOException {

        Admin admin = connection.getAdmin();
        List<TableDescriptor> tableDescriptorList = admin.listTableDescriptors();
        tableDescriptorList.forEach(System.out::println);
    }

    /**
     * 获取多个版本的数据
     * @throws IOException
     */
    @Test
    public void testGetDataVersions() throws IOException {
        TableName tableName = TableName.valueOf("t_user");
        Table table = connection.getTable(tableName);
        Get get = new Get(Bytes.toBytes("u001"));
        get.readAllVersions();
//        get.setTimeRange(0L,System.currentTimeMillis());
        Result result = table.get(get);

        List<Cell> cells = result.getColumnCells(Bytes.toBytes("role"),Bytes.toBytes("rolename"));

        for (Cell c : cells) {
            System.out.println(Bytes.toString(CellUtil.cloneRow(c))
                    + "==> " + Bytes.toString(CellUtil.cloneFamily(c))
                    + "{" + Bytes.toString(CellUtil.cloneQualifier(c))
                    + ":" + Bytes.toString(CellUtil.cloneValue(c))
                    + ", timestamp = " + c.getTimestamp() + "}"
            );
        }
        System.out.println("-----------------");
    }


    @Test
    public void testGetByKey() throws IOException {
        TableName tableName = TableName.valueOf("t_user");
        Table table = connection.getTable(tableName);

        long begin = System.currentTimeMillis();
        Get get = new Get
                (Bytes.toBytes("3"));
        Result result = table.get(get);

        for (Cell c : result.rawCells()) {
            System.out.println(Bytes.toString(CellUtil.cloneRow(c))
                    + "==> " + Bytes.toString(CellUtil.cloneFamily(c))
                    + "{" + Bytes.toString(CellUtil.cloneQualifier(c))
                    + ":" + Bytes.toString(CellUtil.cloneValue(c)) + "}"
            );
        }
        System.out.println(System.currentTimeMillis()-begin);
        System.out.println("-----------------");
    }

    @Test
    public void testScanAll() throws IOException {
        TableName tableName = TableName.valueOf("t_user");
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            for (Cell c : r.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(c))
                        + " ==> " + Bytes.toString(CellUtil.cloneFamily(c))
                        + " {" + Bytes.toString(CellUtil.cloneQualifier(c))
                        + ":" + Bytes.toString(CellUtil.cloneValue(c))
                        + "}");
            }
        }
    }

    @Test
    public void putData() throws IOException {
        Put put = new Put(Bytes.toBytes(3));
//        put.addColumn(Bytes.toBytes("role"), Bytes.toBytes("rolename"), Bytes.toBytes("wangwu"));
        put.addColumn(Bytes.toBytes("role"), Bytes.toBytes("age"), Bytes.toBytes(30));

        TableName tableName = TableName.valueOf("t_user");
        Table table = connection.getTable(tableName);

        table.put(put);
    }

}
