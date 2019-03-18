package streaming.window;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import streaming.dashboard.GeneData;

public class Mytest {

    private String a;


    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream ds = env.addSource(new GeneData());
        ds.flatMap(new RichFlatMapFunction() {
            @Override
            public void flatMap(Object value, Collector out) throws Exception {

            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }
        });

    }



}
