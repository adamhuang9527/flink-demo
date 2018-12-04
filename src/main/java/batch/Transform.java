package batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple8;
import org.junit.jupiter.api.Test;

public class Transform {

	@Test
	public void test0() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		DataSet<org.apache.flink.api.java.tuple.Tuple8<Long, String, String, String, String, String, String, Integer>> ds = env
				.readCsvFile("/Users/user/work/flink_data/ui1.txt").fieldDelimiter("\t").types(Long.class, String.class,
						String.class, String.class, String.class, String.class, String.class, Integer.class);

		DataSet<Tuple8<Long, String, String, String, String, String, String, Integer>> result = ds.map(
				new MapFunction<Tuple8<Long, String, String, String, String, String, String, Integer>, Tuple8<Long, String, String, String, String, String, String, Integer>>() {

					@Override
					public Tuple8<Long, String, String, String, String, String, String, Integer> map(
							Tuple8<Long, String, String, String, String, String, String, Integer> value)
							throws Exception {
						value.f0 = 4000L;
						return value;
					}
				});

		result.writeAsCsv("/Users/user/work/flink_data/result", "\n", "\t");
		env.execute();

	}

	@Test
	public void test1() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		DataSet<UI> ds = env.readCsvFile("/Users/user/work/flink_data/ui1.txt").fieldDelimiter("\t").pojoType(UI.class,
				"datestamp", "id", "province", "city", "page", "biz", "cspot", "day");

		DataSet<UI> result = ds.map(new MapFunction<UI, UI>() {

			@Override
			public UI map(UI value) throws Exception {
				value.setDatestamp(1234L);
				return value;
			}

		});

//		result.writeAsCsv("/Users/user/work/flink_data/result", "\n", "\t");
//		env.execute();
		result.print();
	}

}
