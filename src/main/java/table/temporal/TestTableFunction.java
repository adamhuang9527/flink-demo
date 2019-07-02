package table.temporal;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

public class TestTableFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.registerFunction("split", new Split(" "));



        List<Tuple2<Long, String>> ordersData = new ArrayList<>();
        ordersData.add(Tuple2.of(2L, "Euro"));
        ordersData.add(Tuple2.of(1L, "US Dollar"));
        ordersData.add(Tuple2.of(50L, "Yen"));
        ordersData.add(Tuple2.of(3L, "Euro"));

        DataStream<Tuple2<Long, String>> ordersDataStream = env.fromCollection(ordersData);
        Table orders = tEnv.fromDataStream(ordersDataStream, "amount, currency, proctime.proctime");
        tEnv.registerTable("Orders", orders);


        Table result = tEnv.sqlQuery("SELECT currency, word, length FROM Orders LEFT JOIN LATERAL TABLE(split(currency)) as T(word, length) ON TRUE");
        tEnv.toAppendStream(result, Row.class).print();
        env.execute();
    }




    public static class Split extends TableFunction<Tuple2<String, Integer>> {
        private String separator = ",";

        public Split(String separator) {
            this.separator = separator;
        }

        public void eval(String str) {
            for (String s : str.split(separator)) {
                // use collect(...) to emit a row
                collect(new Tuple2<String, Integer>(s, s.length()));
            }
        }
    }
}
