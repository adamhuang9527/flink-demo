package streaming.connected;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Before;
import org.junit.Test;

public class ConnectTest {

	private DataStream<String> dataStream1;
	private DataStream<String> dataStream2;
	private StreamExecutionEnvironment env;

	@Before
	public void setUp() {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		dataStream1 = env.fromElements("c1", "c2", "c3");
		dataStream2 = env.fromElements("b1", "b2");
	}

	@Test
	public void test1() throws Exception {
		dataStream1.connect(dataStream2).map(new CoMapFunction<String, String, String>() {

			@Override
			public String map1(String value) throws Exception {
				return value+"*";
			}

			@Override
			public String map2(String value) throws Exception {
				return value+"$";
			}
		}).print();

		env.execute();

	}

}
