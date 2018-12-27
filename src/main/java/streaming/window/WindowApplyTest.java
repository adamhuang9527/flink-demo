package streaming.window;

import java.util.List;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import streaming.GeneUISource;

public class WindowApplyTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.addSource(new GeneUISource()).keyBy(new KeySelector<List, String>() {

			@Override
			public String getKey(List value) throws Exception {
				return value.get(0).toString();
			}

		}).timeWindow(Time.seconds(5))
				.apply(new WindowFunction<List, Tuple3<Long, String, Long>, String, TimeWindow>() {

					@Override
					public void apply(String key, TimeWindow window, Iterable<List> input,
							Collector<Tuple3<Long, String, Long>> out) throws Exception {

						long max = 0;
						for (List t : input) {
							long tmp = Long.parseLong(t.get(4).toString());
							if (tmp > max) {
								max = tmp;
							}
						}
						out.collect(new Tuple3<Long, String, Long>(window.getStart(), key, max));
					}
				}).print();

		env.execute();
	}
}
