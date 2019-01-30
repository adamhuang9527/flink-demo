package streaming.table;

/**
 *使用外部表和外部sink
 */
public class ExtSql {
//    public static void main(String[] args)  {


//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
//
//        tableEnv.registerExternalCatalog();
//
//        tableEnv
//                // declare the external system to connect to
//                .connect(
//                        new Kafka()
//                                .version("0.10")
//                                .topic("test-input")
//                                .startFromEarliest()
//                                .property("zookeeper.connect", "localhost:2181")
//                                .property("bootstrap.servers", "localhost:9092")
//                )
//
//                // declare a format for this system
//                .withFormat(
//                        new Avro()
//                                .avroSchema(
//                                        "{" +
//                                                "  \"namespace\": \"org.myorganization\"," +
//                                                "  \"type\": \"record\"," +
//                                                "  \"name\": \"UserMessage\"," +
//                                                "    \"fields\": [" +
//                                                "      {\"name\": \"timestamp\", \"type\": \"string\"}," +
//                                                "      {\"name\": \"user\", \"type\": \"long\"}," +
//                                                "      {\"name\": \"message\", \"type\": [\"string\", \"null\"]}" +
//                                                "    ]" +
//                                                "}"
//                                )
//                )
//
//                // declare the schema of the table
//                .withSchema(
//                        new Schema()
//                                .field("rowtime", Types.SQL_TIMESTAMP)
//                                .rowtime(new Rowtime()
//                                        .timestampsFromField("timestamp")
//                                        .watermarksPeriodicBounded(60000)
//                                )
//                                .field("user", Types.LONG)
//                                .field("message", Types.STRING)
//                )
//
//                // specify the update-mode for streaming tables
//                .inAppendMode()
//
//                // register as source, sink, or both and under a name
//                .registerTableSource("MyUserTable");
//
//    }

}
