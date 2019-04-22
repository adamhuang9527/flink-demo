package streaming.table;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * 使用外部表和外部sink
 */
public class ExtSql {
    public static void main(String[] args) throws Exception {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
//        env.setStateBackend(new RocksDBStateBackend("hdfs://localhost/checkpoints-data/"));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);




        // declare the external system to connect to
        tableEnv.connect(
                new Kafka()
                        .version("0.10")
                        .topic("test4")
                        .startFromEarliest()
                        .property("bootstrap.servers", "localhost:9092")
                        .property("group.id", "mygroup")
        ).withFormat(new Json().failOnMissingField(false).deriveSchema())
                // declare the schema of the table
                .withSchema(
                        new Schema()
                                .field("appName", Types.STRING())
                                .field("clientIp", Types.STRING())
                                .field("rowtime", Types.SQL_TIMESTAMP())
                                .rowtime(new Rowtime()
                                        .timestampsFromField("uploadTime")
                                        .watermarksPeriodicBounded(60000)
                                )

                )

                // specify the update-mode for streaming tables
                .inAppendMode()
                // register as source, sink, or both and under a name
                .registerTableSource("MyUserTable");


//        Table result = tableEnv.sqlQuery("select * from MyUserTable");
          Table result = tableEnv.sqlQuery("select clientIp,count(1) from MyUserTable group by clientIp");
//        Table result = tableEnv.sqlQuery("select clientIp,TUMBLE_START(rowtime, INTERVAL '10' SECOND) as wStart,count(1) from MyUserTable group by clientIp,TUMBLE(rowtime, INTERVAL '10' SECOND)");



//        TableSink sink = new CsvTableSink("/Users/user/work/flink_data/data",",");
//        // define the field names and types
//        String[] fieldNames = {"a", "b"};
//        TypeInformation[] fieldTypes = {Types.STRING(), Types.INT()};
//        tableEnv.registerTableSink("result",fieldNames, fieldTypes,sink);

//        result.insertInto("result");
        tableEnv.toAppendStream(result, Row.class).print();
//        tableEnv.toRetractStream(result, Row.class).print();
        env.execute("example");

    }

}
