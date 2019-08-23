package table;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class StreamJoinSql {
    public static void main(String[] args) {

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Tuple3<Long, String, Integer>> orderA = env.fromCollection(Arrays.asList(
                new Tuple3<>(1L, "beer", 3),
                new Tuple3<>(1L, "diaper", 4),
                new Tuple3<>(3L, "rubber", 2),
                new Tuple3<>(3L, "rubber", 2)));
        tEnv.registerDataStream("OrderA", orderA, "id,product,amount");

        DataStream<Tuple3<Long, String, Integer>> orderB = env.fromCollection(Arrays.asList(
                new Tuple3<>(1L, "pen", 3),
                new Tuple3<>(3L, "rubber", 3),
                new Tuple3<>(4L, "beer", 1)));
        // register DataStream as Table
        tEnv.registerDataStream("OrderB", orderB, "id,product,amount");

        // union the two tables
        Table result = tEnv.sqlQuery("SELECT * FROM OrderA as a left join OrderB  as b on a.id = b.id ");

        tEnv.toRetractStream(result, Row.class).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
