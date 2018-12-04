package org.apache.flink.streaming.examples.watermark;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

public class Test1 {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<String> ds = env.addSource(new SourceFunction<String>() {

			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				// TODO Auto-generated method stub
//				ctx.collectWithTimestamp(element, timestamp);
				ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
			}

			@Override
			public void cancel() {
				// TODO Auto-generated method stub

			}

		});

		ds.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {

			@Override
			public long extractTimestamp(String element, long previousElementTimestamp) {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public Watermark getCurrentWatermark() {
				// TODO Auto-generated method stub
				return null;
			}
		});

		ds.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks() {

			@Override
			public long extractTimestamp(Object element, long previousElementTimestamp) {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public Watermark checkAndGetNextWatermark(Object lastElement, long extractedTimestamp) {
				// TODO Auto-generated method stub
				return null;
			}
		});

	}
}
