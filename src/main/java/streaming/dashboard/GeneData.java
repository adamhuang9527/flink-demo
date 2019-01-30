package streaming.dashboard;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.UUID;

public class GeneData implements SourceFunction<String> {

    private volatile boolean isRunning = true;

    static int status[] = {200, 404, 500, 501, 301};

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        while (isRunning) {
            Thread.sleep((int) (Math.random() * 5));
            // traceid,userid,timestamp,status,response time


            StringBuffer ss = new StringBuffer();
            ss.append(UUID.randomUUID().toString());
            ss.append(",");
            ss.append((int) (Math.random() * 100000));
            ss.append(",");
            ss.append(System.currentTimeMillis());
            ss.append(",");
            ss.append(status[(int) (Math.random() * 4)]);
            ss.append(",");
            ss.append((int) (Math.random() * 200));

            ctx.collect(ss.toString());
        }

    }

    @Override
    public void cancel() {

    }
}
