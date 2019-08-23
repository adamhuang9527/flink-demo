package ddl;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;

public class DDLTCase {

	StreamExecutionEnvironment env;
	StreamTableEnvironment tEnv;

	@Before
	public void init() {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		env.enableCheckpointing(5000);
		tEnv = StreamTableEnvironment.create(env);
	}

	@Test
	public void testKafka2Kafka() throws Exception {

		String sql = "CREATE TABLE tbl1 (" +
				"  appName varchar," +
				"  clientIp varchar, " +
				"  uploadTime bigint " +
				")" +
				" comment 'test table comment ABC.'" +
				"  with (" +
				"    'connector.type' = 'kafka', " +
				"    'connector.version' = '0.10', " +
				"    'update-mode' = 'append', " +
				"    'connector.properties.0.key' = 'bootstrap.servers', " +
				"    'connector.properties.0.value' = 'localhost:9092', " +
				"    'format.type' = 'json', " +
				"    'format.derive-schema' = 'true', " +
				"    'connector.topic' = 'test3'" +
				")";

		tEnv.sqlUpdate(sql);
		Table table = tEnv.sqlQuery("select * from tbl1");
		tEnv.toAppendStream(table, Row.class).print();
		env.execute();
	}





	@Test
	public void test() throws Exception {
		tEnv.connect(new Kafka()
				.version("0.10")
				.topic("kafkaSourceJson").startFromLatest()
//				.property("bootstrap.servers", "localhost:9092")
		    )
				.withFormat(new Json().deriveSchema())
				.withSchema(new Schema()
						.field("num", Types.INT)
						.field("city", Types.STRING)
						.field("state", Types.STRING)
						.field("zip", Types.STRING)
						.field("street", Types.STRING)
				)
				.inAppendMode()
				.registerTableSource("mysource");

		Table table = tEnv.sqlQuery("select * from mysource");
		tEnv.toAppendStream(table, Row.class).print();
		env.execute();
	}
}
