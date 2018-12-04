package batch.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class MapTest {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<String> data = env.fromElements("1", "2");

		DataSet<Integer> result = data.map(new MapFunction<String, Integer>() {
			public Integer map(String value) {
				return Integer.parseInt(value)+110;
			}
		});
		
		result.print();

	}
}
