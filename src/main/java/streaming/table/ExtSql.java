package streaming.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * 使用外部表和外部sink
 */
public class ExtSql {
    public static void main(String[] args) throws Exception {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        // declare the external system to connect to
        tableEnv.connect(
                new Kafka()
                        .version("0.10")
                        .topic("test-input")
                        .startFromLatest()
                        .property("bootstrap.servers", "localhost:9092")
                        .property("group.id", "mygroup")
        ).withFormat(new Json().failOnMissingField(false).deriveSchema())
                // declare the schema of the table
                .withSchema(
                        new Schema()
//                                .field("rowtime", Types.SQL_TIMESTAMP)
//                                .rowtime(new Rowtime()
//                                        .timestampsFromField("timestamp")
//                                        .watermarksPeriodicBounded(60000)
//                                )
                                .field("lon", Types.LONG())
                                .field("lat", Types.LONG())
                )

                // specify the update-mode for streaming tables
                .inAppendMode()
                // register as source, sink, or both and under a name
                .registerTableSource("MyUserTable");

        Table result = tableEnv.sqlQuery("select properties.lat,count(1) from MyUserTable group by properties.lat");


        tableEnv.toRetractStream(result, Row.class).print();
        env.execute("example");

    }

}
