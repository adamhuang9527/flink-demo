package streaming.tablefunction;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import streaming.GeneUISource;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


/**
 *
 */
public class TestTemFunction {
    public static void main(String[] args) throws Exception {


        // Get the stream and table environments.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


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
        String[] fieldNames = new String[]{"currency", "rate", "rateid"};
        TypeInformation[] fieldTypes = new TypeInformation[]{Types.STRING(), Types.INT(), Types.INT()};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
        MySQLTableFunction split = new MySQLTableFunction(rowTypeInfo, "jdbc:mysql://localhost/test", "root", "root", "product", "rateid");
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
        tEnv.toAppendStream(result, Row.class).print();
        env.execute();
    }

    public static class MySQLTableFunction extends TableFunction<Row> {

        //从外部传进来的参数
        private String url;
        private String username;
        private String password;
        private String tableName;
        private Object primaryKey;
        private RowTypeInfo rowTypeInfo;
        private int cacheSize;
        private int cacheTTLMs;

        private static final int cacheSizeDefaultValue = 1000;
        private static final int cacheTTLMsDefaultValue = 1000 * 60 * 60;

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;


        String[] fileFields = null;
        public LoadingCache<Object, List<Row>> funnelCache = null;

        private List<Row> getRowData(Object primaryKey) throws SQLException {
            int fieldCount = fileFields.length;
            ps.setObject(1, primaryKey);
            rs = ps.executeQuery();
            List<Row> rowList = new ArrayList<>();
            while (rs.next()) {
                Row row = new Row(fieldCount);
                for (int i = 0; i < fieldCount; i++) {
                    row.setField(i, rs.getObject(i + 1));
                }
                rowList.add(row);
            }
            return rowList;
        }


        public MySQLTableFunction(RowTypeInfo rowTypeInfo, String url, String username, String password, String tableName, Object primaryKey) {
            this(rowTypeInfo, url, username, password, tableName, primaryKey, cacheSizeDefaultValue, cacheTTLMsDefaultValue);
        }


        public MySQLTableFunction(RowTypeInfo rowTypeInfo, String url, String username, String password, String tableName, Object primaryKey, int cacheSize, int cacheTTLMs) {
            this.rowTypeInfo = rowTypeInfo;
            this.url = url;
            this.username = username;
            this.password = password;
            this.tableName = tableName;
            this.primaryKey = primaryKey;
            this.cacheSize = cacheSize;
            this.cacheTTLMs = cacheTTLMs;
        }

        public void eval(Object primaryKey) throws ExecutionException {
            List<Row> rowList = funnelCache.get(primaryKey);
            for (int i = 0; i < rowList.size(); i++) {
                collect(rowList.get(i));
            }

        }


        @Override
        public TypeInformation<Row> getResultType() {
            return org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(rowTypeInfo.getFieldNames(), rowTypeInfo.getFieldTypes());
        }


        @Override
        public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
            return new TypeInformation[]{Types.INT()};
        }


        @Override
        public void open(FunctionContext context) throws Exception {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, username, password);
            fileFields = rowTypeInfo.getFieldNames();

            String fields = StringUtils.join(fileFields, ",");
            StringBuilder ss = new StringBuilder();
            ss.append("SELECT ").append(fields).append(" FROM ").append(tableName).append(" where ").append(primaryKey).append(" =  ? ");
            ps = conn.prepareStatement(ss.toString());

            funnelCache = CacheBuilder.newBuilder().maximumSize(cacheSize)
                    .expireAfterAccess(cacheTTLMs, TimeUnit.MILLISECONDS).build(new CacheLoader<Object, List<Row>>() {
                        @Override
                        public List<Row> load(Object primaryKey) throws Exception {
                            return getRowData(primaryKey);
                        }
                    });
        }

        @Override
        public void close() throws Exception {
            rs.close();
            ps.close();
            conn.close();
        }
    }


}
