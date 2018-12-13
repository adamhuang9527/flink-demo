package streaming.connected;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Before;
import org.junit.Test;

public class JoinTest {
	private DataStream<Tuple3<String, Long, String>> dataStream1;
	private DataStream<Tuple3<String, Long, Long>> dataStream2;
	private TumblingEventTimeWindows tsAssigner;
	private StreamExecutionEnvironment env;

	@Before
	public void setUp() {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		dataStream1 = env.addSource(new Source1())
				.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple3<String, Long, String>>() {

					@Override
					public long extractTimestamp(Tuple3<String, Long, String> element, long previousElementTimestamp) {
						return element.f1;
					}

					@Override
					public Watermark checkAndGetNextWatermark(Tuple3<String, Long, String> lastElement,
							long extractedTimestamp) {
						return new Watermark(lastElement.f1);
					}
				});
		dataStream2 = env.addSource(new Source2())
				.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple3<String, Long, Long>>() {

					@Override
					public long extractTimestamp(Tuple3<String, Long, Long> element, long previousElementTimestamp) {
						return element.f1;
					}

					@Override
					public Watermark checkAndGetNextWatermark(Tuple3<String, Long, Long> lastElement,
							long extractedTimestamp) {
						return new Watermark(lastElement.f1);
					}
				});
		tsAssigner = TumblingEventTimeWindows.of(Time.milliseconds(5));
	}

	@Test
	public void test1() throws Exception {

		dataStream1.join(dataStream2).where(new KeySelector<Tuple3<String, Long, String>, String>() {

			@Override
			public String getKey(Tuple3<String, Long, String> value) throws Exception {

				return value.f0;
			}
		}).equalTo(new KeySelector<Tuple3<String, Long, Long>, String>() {

			@Override
			public String getKey(Tuple3<String, Long, Long> value) throws Exception {
				return value.f0;
			}
		}).window(tsAssigner).apply(
				new JoinFunction<Tuple3<String, Long, String>, Tuple3<String, Long, Long>, Tuple3<String, String, Long>>() {

					@Override
					public Tuple3<String, String, Long> join(Tuple3<String, Long, String> first,
							Tuple3<String, Long, Long> second) throws Exception {
						return new Tuple3<String, String, Long>(first.f0, first.f2, second.f2);
					}
				}).print();

		env.execute();
	}

	class Source1 implements SourceFunction<Tuple3<String, Long, String>> {
		private volatile boolean isRunning = true;
		private static final long serialVersionUID = 1L;
		String province[] = new String[] { "上海", "云南", "内蒙", "北京", "吉林", "四川", "国外", "天津", "宁夏", "安徽", "山东", "山西", "广东",
				"广西", "江苏", "江西", "河北", "河南", "浙江", "海南", "湖北", "湖南", "甘肃", "福建", "贵州", "辽宁", "重庆", "陕西", "香港", "黑龙江" };

		@Override
		public void run(SourceContext<Tuple3<String, Long, String>> ctx) throws Exception {
			while (isRunning) {
				Thread.sleep(10);
				String id = "id-" + (int) (Math.random() * 100);
				String pro = province[(int) (Math.random() * 29)];
				Tuple3<String, Long, String> t = new Tuple3<String, Long, String>(id, System.currentTimeMillis(), pro);
				ctx.collect(t);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

	}

	class Source2 implements SourceFunction<Tuple3<String, Long, Long>> {
		private volatile boolean isRunning = true;
		private static final long serialVersionUID = 1L;
		private AtomicLong al = new AtomicLong(0L);

		@Override
		public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
			while (isRunning) {
				Thread.sleep(10);
				String id = "id-" + (int) (Math.random() * 100);
				Tuple3<String, Long, Long> t = new Tuple3<String, Long, Long>(id, System.currentTimeMillis(),
						al.incrementAndGet());
				ctx.collect(t);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

	}

}
