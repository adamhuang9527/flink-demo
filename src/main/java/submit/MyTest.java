package submit;

import java.io.File;
import java.net.URL;

public class MyTest {
    public static void main(String[] args) throws Exception {


        System.out.println(new URL("file:///Users/user/git/flink-udf/target/flink-udf-1.0.jar"));

        System.out.println(new File("/Users/user/git/flink-udf/target/flink-udf-1.0.jar").toURI());

        System.out.println(111);

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000);
//        DataStream<String> ds = env.addSource(new SourceFunction<String>() {
//
//            private volatile boolean isRunning = true;
//            AtomicLong al = new AtomicLong();
//
//            @Override
//            public void run(SourceContext<String> ctx) throws Exception {
//                while (isRunning) {
//                    Thread.sleep(10);
//                    ctx.collect("value" + al.incrementAndGet());
//                }
//            }
//
//            @Override
//            public void cancel() {
//                isRunning = false;
//            }
//        });
//
//        ds.print();
//
//
//        System.out.println(new JSONGenerator(env.getStreamGraph()).getJSON());
//
//        env.execute();
    }
}
