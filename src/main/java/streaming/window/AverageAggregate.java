package streaming.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public  class AverageAggregate implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {

	@Override
	public Tuple2<Long, Long> createAccumulator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double getResult(Tuple2<Long, Long> accumulator) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
		// TODO Auto-generated method stub
		return null;
	}

}