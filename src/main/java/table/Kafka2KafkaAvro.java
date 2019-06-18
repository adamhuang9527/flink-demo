package table;

public class Kafka2KafkaAvro {
    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//
//        tableEnv.connect(
//                new Kafka()
//                        .version("0.10")
//                        .topic("kafkaSinkAvro")
//                        .property("bootstrap.servers", "localhost:9092"))
//                .withFormat(new Avro().recordClass(Address.class))
//                .withSchema(new Schema().field("a", Types.INT())
//                        .field("b", Types.STRING())
//                        .field("c", Types.STRING())
//                        .field("d", Types.STRING())
//                        .field("e", Types.STRING())
//                )
//                .inAppendMode()
//                .registerTableSink("mysink");
    }
}
