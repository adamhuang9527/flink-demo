package batch;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;

/**
 * group and sort
 * 
 * @author user
 *
 */
public class GroupSort {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);
		;

		DataSet<org.apache.flink.api.java.tuple.Tuple8<Long, String, String, String, String, String, String, Integer>> ds = env
				.readCsvFile("/Users/user/work/flink_data/ui1.txt").fieldDelimiter("\t").types(Long.class, String.class,
						String.class, String.class, String.class, String.class, String.class, Integer.class);

		DataSet result = ds.groupBy(1).sortGroup(0, Order.ASCENDING).reduceGroup(
				new GroupReduceFunction<Tuple8<Long, String, String, String, String, String, String, Integer>, Tuple8<Long, String, String, String, String, String, String, Integer>>() {

					@Override
					public void reduce(
							Iterable<Tuple8<Long, String, String, String, String, String, String, Integer>> values,
							Collector<Tuple8<Long, String, String, String, String, String, String, Integer>> out)
							throws Exception {
						values.forEach(v -> out.collect(v));

					}
				});

		
		result.print();
	}

}
