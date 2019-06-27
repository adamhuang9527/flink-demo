package streaming.tablefunction;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import streaming.GeneUISource;

import java.util.List;


/**
 *
 */
public class TestTemFunction {
    StreamExecutionEnvironment env = null;
    StreamTableEnvironment tEnv = null;

    @Before
    public void init() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        tEnv = StreamTableEnvironment.create(env);
    }


    @Test
    public void testMysql() throws Exception {


        /**
         *
         *
         * create table mysqlTable(
         *     currency varchar,
         *     rate int,
         *     rateid int,
         *     PRIMARY KEY(rateid),
         *     PERIOD FOR SYSTEM_TIME
         *  )WITH(
         *     type='mysql',
         *     url='jdbc:mysql://localhost:3306/test?charset=utf8',
         *     userName='root',
         *     password='root',
         *     tableName='product',
         *     cache ='LRU',
         *     cacheSize ='100',
         *     cacheTTLMs ='6000'
         *  );
         */
        //维表的字段，字段名称和类型要和mysql的一样，否则会查不出来
        String[] fieldNames = new String[]{"cuccency", "rate", "id", "province"};
        TypeInformation[] fieldTypes = new TypeInformation[]{Types.STRING(), Types.INT(), Types.INT(),Types.STRING()};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
        String[] primaryKeys =  new String[]{"id"};
        MySQLTableFunction split = new MySQLTableFunction(rowTypeInfo, "jdbc:mysql://localhost/test", "root", "root", "product1", primaryKeys);
        tEnv.registerFunction("mysql", split);


//        List<Tuple3<Long, String, Long>> ordersData = new ArrayList<>();
//        ordersData.add(Tuple3.of(2L, "Euro", 2L));
//        ordersData.add(Tuple3.of(1L, "US Dollar", 3L));
//        ordersData.add(Tuple3.of(50L, "Yen", 4l));
//        ordersData.add(Tuple3.of(3L, "Euro", 5L));
//        DataStream<Tuple3<Long, String, Long>> ordersDataStream = env.fromCollection(ordersData);
//        Table orders = tEnv.fromDataStream(ordersDataStream, "id,currency,amount,o_proctime.proctime");
//        tEnv.registerTable("Orders", orders);


//        省市、id、datestamp、date、计数,
        DataStream<Tuple5<String, Integer, Long, String, Long>> data = env.addSource(new GeneUISource())
                .map(new MapFunction<List, Tuple5<String, Integer, Long, String, Long>>() {
                    @Override
                    public Tuple5<String, Integer, Long, String, Long> map(List value) throws Exception {
                        return new Tuple5<>(value.get(0).toString(),
                                Integer.parseInt(value.get(1).toString()), Long.parseLong(value.get(2).toString()),
                                value.get(3).toString(), Long.parseLong(value.get(4).toString()));
                    }
                });

        tEnv.registerDataStream("userinfo", data, "province,id,datastamp,date,num");

        String sql = "SELECT u.* , r.* , u.num * r.rate FROM userinfo  as u " +
                " left JOIN LATERAL TABLE(mysql(u.id)) as r ON true";
        Table result = tEnv.sqlQuery(sql);
        tEnv.toRetractStream(result, Row.class).print();
        env.execute();
    }


    @Test
    public void testHbase() throws Exception {
        String[] fieldNames = new String[]{"id", "role.age", "role.name"};
        TypeInformation[] fieldTypes = new TypeInformation[]{Types.INT(), Types.INT(), Types.STRING()};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);

        HbaseTableFunction hbase = new HbaseTableFunction(rowTypeInfo, "10.160.85.185", "t_user", "id");
        tEnv.registerFunction("hbase", hbase);


        //        省市、id、datestamp、date、计数,
        DataStream<Tuple5<String, Integer, Long, String, Long>> data = env.addSource(new GeneUISource())
                .map(new MapFunction<List, Tuple5<String, Integer, Long, String, Long>>() {
                    @Override
                    public Tuple5<String, Integer, Long, String, Long> map(List value) throws Exception {
                        return new Tuple5<>(value.get(0).toString(),
                                Integer.parseInt(value.get(1).toString()), Long.parseLong(value.get(2).toString()),
                                value.get(3).toString(), Long.parseLong(value.get(4).toString()));
                    }
                });

        tEnv.registerDataStream("userinfo", data, "province,id,datastamp,date,num");

        String sql = "SELECT u.* , r.`role.age`  FROM userinfo  as u" +
                " left JOIN LATERAL TABLE(hbase(u.id)) as r ON true";
        Table result = tEnv.sqlQuery(sql);

        tEnv.toRetractStream(result, Row.class).print();
        env.execute();

    }
}
