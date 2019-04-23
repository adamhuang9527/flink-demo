package streaming.table.sink;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.TableSink;

import java.util.Map;

public class KafkaTableSink {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);





        // declare the external system to connect to
        tableEnv.connect(
                new Kafka()
                        .version("0.10")
                        .topic("test6")
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



        Kafka kafka = new Kafka()
                .version("0.10")
                .topic("kafkaSink")
                .property("bootstrap.servers", "localhost:9092");
        StreamTableDescriptor std = new StreamTableDescriptor(tableEnv, kafka);

        std.withFormat(new JsonFormarDesc("json",1))
                .withSchema(
                        new Schema()
//                                .field("appName", Types.STRING())
                                .field("clientIp", Types.STRING())
                                .field("rowtime", Types.SQL_TIMESTAMP()))
                .inAppendMode();


        Map<String, String> propertiesMap = std.toProperties();
        TableSink<?> actualSink = TableFactoryService.find(StreamTableSinkFactory.class, propertiesMap)
                .createStreamTableSink(propertiesMap);

        tableEnv.registerTableSink("mysink", actualSink);

        tableEnv.sqlUpdate("insert into mysink select clientIp,rowtime from MyUserTable");

        env.execute();

    }

     static class JsonFormarDesc extends FormatDescriptor {

         /**
          * Constructs a {@link FormatDescriptor}.
          *
          * @param type    string that identifies this format
          * @param version property version for backwards compatibility
          */
         public JsonFormarDesc(String type, int version) {
             super(type, version);
         }

         @Override
        protected Map<String, String> toFormatProperties() {

            DescriptorProperties  properties = new DescriptorProperties();

            properties.putString("format.derive-schema", "true");

            return properties.asMap();
        }
    }


    static class CsvFormarDesc extends FormatDescriptor {

        /**
         * Constructs a {@link FormatDescriptor}.
         *
         * @param type    string that identifies this format
         * @param version property version for backwards compatibility
         */
        public CsvFormarDesc(String type, int version) {
            super(type, version);
        }

        @Override
        protected Map<String, String> toFormatProperties() {

            DescriptorProperties  properties = new DescriptorProperties();

            properties.putString("format.field-delimiter", "#");
            properties.putString("format.derive-schema", "true");

            return properties.asMap();
        }
    }
}
