package batch.table;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import batch.UI;
import org.junit.Test;

public class ReadFromFile {
	@Test
	public void test1() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<UI> ds = env.readCsvFile("/Users/user/work/flink_data/ui.txt").fieldDelimiter("\t").pojoType(UI.class,
				"datestamp", "id", "province", "city", "page", "biz", "cspot", "day");
		ds.filter(new FilterFunction<UI>() {

			@Override
			public boolean filter(UI value) throws Exception {

				return null != value.getProvince() && value.getProvince().length() > 0;
			}
		});

		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

		tEnv.registerDataSet("ui", ds);

		Table result = tEnv.sqlQuery("select province, count(*) as c from ui group by province order by c ");

		TupleTypeInfo<Tuple2<String, Long>> tupleType = new TupleTypeInfo<>(Types.STRING(), Types.LONG());
		DataSet<Tuple2<String, Long>> dsTuple = tEnv.toDataSet(result, tupleType);
		dsTuple.print();

	}
}
