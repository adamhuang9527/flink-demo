package submit;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.JSONGenerator;

import java.util.concurrent.atomic.AtomicLong;

public class MyTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        DataStream<String> ds = env.addSource(new SourceFunction<String>() {

            private volatile boolean isRunning = true;
            AtomicLong al = new AtomicLong();

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isRunning) {
                    Thread.sleep(10);
                    ctx.collect("value" + al.incrementAndGet());
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        ds.print();


        System.out.println(new JSONGenerator(env.getStreamGraph()).getJSON());

        env.execute();
    }
}
