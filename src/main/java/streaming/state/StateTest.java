package streaming.state;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import streaming.GeneUISource;

public class StateTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.addSource(new GeneUISource()).print();
		env.execute();
	}
}
