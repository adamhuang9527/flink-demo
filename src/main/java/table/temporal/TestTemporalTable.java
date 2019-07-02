package table.temporal;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

public class TestTemporalTable {
    public static void main(String[] args) throws Exception {
        // Get the stream and table environments.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        List<Tuple2<Long, String>> ordersData = new ArrayList<>();
        ordersData.add(Tuple2.of(2L, "Euro"));
        ordersData.add(Tuple2.of(1L, "US Dollar"));
        ordersData.add(Tuple2.of(50L, "Yen"));
        ordersData.add(Tuple2.of(3L, "Euro"));

        DataStream<Tuple2<Long, String>> ordersDataStream = env.fromCollection(ordersData);
        Table orders = tEnv.fromDataStream(ordersDataStream, "amount, currency, proctime.proctime");
        tEnv.registerTable("Orders", orders);


        List<Tuple2<String, Long>> ratesHistoryData = new ArrayList<>();
        ratesHistoryData.add(Tuple2.of("US Dollar", 102L));
        ratesHistoryData.add(Tuple2.of("Euro", 114L));
        ratesHistoryData.add(Tuple2.of("Yen", 1L));
        ratesHistoryData.add(Tuple2.of("Euro", 116L));
        ratesHistoryData.add(Tuple2.of("Euro", 119L));

        DataStream<Tuple2<String, Long>> ratesHistoryStream = env.fromCollection(ratesHistoryData);
        Table ratesHistory = tEnv.fromDataStream(ratesHistoryStream, "currency, rate, proctime.proctime");

        tEnv.registerTable("RatesHistory", ratesHistory);

        TemporalTableFunction rates = ratesHistory.createTemporalTableFunction("proctime", "currency"); // <==== (1)
        tEnv.registerFunction("Rates", rates);


        String sql = "SELECT o.currency, o.amount, r.rate ," +
                "o.amount * r.rate AS yen_amount " +
                "FROM " +
                "Orders AS o, " +
                "LATERAL TABLE (Rates(o.proctime)) AS r " +
                "WHERE r.currency = o.currency ";


        String sql1="select * from Orders as o join Rates FOR SYSTEM_TIME AS OF PROCTIME() as r on r.currency = o.currency";

        Table result = tEnv.sqlQuery(sql);
        tEnv.toAppendStream(result, Row.class).print();
        env.execute();
    }
}
