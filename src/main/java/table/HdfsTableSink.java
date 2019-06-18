package table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.time.ZoneId;
import java.util.Arrays;

public class HdfsTableSink implements AppendStreamTableSink {

    /**
     * The schema of the table.
     */
    private final TableSchema schema;

    public HdfsTableSink(TableSchema schema) {
        this.schema = schema;
    }


    @Override
    public void emitDataStream(DataStream dataStream) {

        BucketingSink<String> sink = new BucketingSink<String>("hdfs:///flink-data");
        sink.setBucketer(new DateTimeBucketer<String>("yyyy-MM-dd", ZoneId.of("UTC+8")));
        sink.setWriter(new StringWriter<String>());
        sink.setBatchSize(1024 * 1024 * 10); // this is 10 MB,
        sink.setBatchRolloverInterval(1 * 60 * 1000); // this is 3 mins

        dataStream.addSink(sink);
    }


    @Override
    public TypeInformation<Row> getOutputType() {
        return schema.toRowType();
    }

    @Override
    public String[] getFieldNames() {
        return schema.getFieldNames();
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return schema.getFieldTypes();
    }


    @Override
    public TableSink configure(String[] fieldNames, TypeInformation[] fieldTypes) {
        if (!Arrays.equals(getFieldNames(), fieldNames) || !Arrays.equals(getFieldTypes(), fieldTypes)) {
            throw new ValidationException("Reconfiguration with different fields is not allowed. " +
                    "Expected: " + Arrays.toString(getFieldNames()) + " / " + Arrays.toString(getFieldTypes()) + ". " +
                    "But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
        }
        return this;
    }


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStateBackend(new RocksDBStateBackend("hdfs:///checkpoints-data/"));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // declare the external system to connect to
        tableEnv.connect(
                new Kafka()
                        .version("0.10")
                        .topic("test3")
                        .startFromLatest()
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



        char c = 0x01;
        tableEnv.connect(new FileSystem().path("file:///tmp/flink-data/"))
                .withFormat(new OldCsv().fieldDelimiter("|")
                                        .field("appName", Types.STRING())
                                        .field("clientIp", Types.STRING())
                                        .field("rowtime", Types.SQL_TIMESTAMP()))
                .withSchema(new Schema()
                        .field("appName", Types.STRING())
                        .field("clientIp", Types.STRING())
                        .field("rowtime", Types.SQL_TIMESTAMP()))
                .inAppendMode()
                .registerTableSink("myHdfsSink");

//        HdfsTableSink sink = new HdfsTableSink(schema);
//        tableEnv.registerTableSink("myHdfsSink", sink);


        tableEnv.sqlUpdate("insert into myHdfsSink select * from MyUserTable");


//        Table result = tableEnv.sqlQuery("select * from MyUserTable");
//        BucketingSink<Row> sink = new BucketingSink<>("hdfs://localhost/flink_data");
//        sink.setBucketer(new DateTimeBucketer<Row>("yyyy-MM-dd", ZoneId.of("UTC+8")));
//        sink.setWriter(new StringWriter<Row>());
//        sink.setBatchSize(1024 * 1024 * 10); // this is 10 MB,
//        sink.setBatchRolloverInterval(1 * 60 * 1000); // this is 3 mins
//        tableEnv.toAppendStream(result,Row.class).addSink(sink);


        env.execute();
    }
}
