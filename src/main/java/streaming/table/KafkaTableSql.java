package streaming.table;

public class KafkaTableSql {
//    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//
//        String[] fieldNames = new String[]{"appName","clientIp"};
//        TypeInformation<String>[] fieldTypes = new TypeInformation[]{Types.STRING(),Types.STRING()};
//
//        TableSchema tableSchema = new TableSchema(fieldNames,fieldTypes);
//
//
//
//        Map<String, String> properties = new HashMap();
//        properties.put("appName","appName");
//        properties.put("clientIp","clientIp");
//        DescriptorProperties descriptorProperties = getValidatedProperties(properties);
//        JsonRowDeserializationSchema schema = new JsonRowDeserializationSchema(createTypeInformation(descriptorProperties));
//        descriptorProperties.getOptionalBoolean(JsonValidator.FORMAT_FAIL_ON_MISSING_FIELD)
//                .ifPresent(schema::setFailOnMissingField);x
//
//
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//        properties.setProperty("group.id", "test");
//
//        Kafka010TableSource source = new Kafka010TableSource(tableSchema,"test4",properties,schema);
//
//
//        tableEnv.registerTableSource("kafka_test1",source);
//    }
//
//
//    private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
//        final DescriptorProperties descriptorProperties = new DescriptorProperties();
//        descriptorProperties.putProperties(propertiesMap);
//
//        // validate
//        new JsonValidator().validate(descriptorProperties);
//
//        return descriptorProperties;
//    }
}
