package streaming.udf;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.Test;
import streaming.dashboard.GeneData;

public class MySpeedTest {

    StreamExecutionEnvironment env = null;
    StreamTableEnvironment tEnv = null;

    @Before
    public void init() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        tEnv = StreamTableEnvironment.create(env);
    }


    @Test
    public void test1() throws Exception {
        env.addSource(new GeneData()).print();
        env.execute();
    }

    @Test
    public void test2() throws Exception {
        env.addSource(new GeneData()).map((MapFunction<String, Object>) value -> {
            Thread.sleep(500);
            return value;
        }).print();
        env.execute();
    }

}
