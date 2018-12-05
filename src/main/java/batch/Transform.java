package batch;

import java.text.SimpleDateFormat;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple8;
import org.junit.Test;

public class Transform {

	/**
	 * 主要功能是将第一个字段的yyyyMMddHHmmssSSS类型转化成long形状的时间戳格式
	 * 
	 * @throws Exception
	 */
	@Test
	public void test0() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");

		DataSet<org.apache.flink.api.java.tuple.Tuple8<Long, String, String, String, String, String, String, Integer>> ds = env
				.readCsvFile("/Users/user/work/flink_data/ui.txt").fieldDelimiter("\t").types(Long.class, String.class,
						String.class, String.class, String.class, String.class, String.class, Integer.class);

		ds.filter(new FilterFunction<Tuple8<Long, String, String, String, String, String, String, Integer>>() {

			@Override
			public boolean filter(Tuple8<Long, String, String, String, String, String, String, Integer> value)
					throws Exception {
				return null != value.f2 && value.f2.length() > 0;
			}

		});

		DataSet<Tuple8<Long, String, String, String, String, String, String, Integer>> result = ds.map(
				new MapFunction<Tuple8<Long, String, String, String, String, String, String, Integer>, Tuple8<Long, String, String, String, String, String, String, Integer>>() {

					@Override
					public Tuple8<Long, String, String, String, String, String, String, Integer> map(
							Tuple8<Long, String, String, String, String, String, String, Integer> value)
							throws Exception {
						value.f0 = sdf.parse(value.f0.toString()).getTime();
						return value;
					}
				});

		result.writeAsCsv("/Users/user/work/flink_data/result", "\n", "\t");
		env.execute();

	}

//	@Test
//	public void test1() throws Exception {
//
//		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//
////		env.setParallelism(1);
//
//		DataSet<UI> ds = env.readCsvFile("/Users/user/work/flink_data/ui1.txt").fieldDelimiter("\t").pojoType(UI.class,
//				"datestamp", "id", "province", "city", "page", "biz", "cspot", "day");
//
//		DataSet<UI> result = ds.map(new MapFunction<UI, UI>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public UI map(UI value) throws Exception {
////				value.setDatestamp(sdf.parse(value.getDatestamp().toString()).getTime());
//				return value;
//			}
//
//		});
//
//		result.print();
//	}

}
