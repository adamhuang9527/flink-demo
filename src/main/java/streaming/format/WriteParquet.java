package streaming.format;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.Serializable;

public class WriteParquet {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStateBackend(new RocksDBStateBackend("hdfs://localhost/checkpoints-data/", true));


        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        DataStream<Datum> ds = env.addSource(new SourceFunction<Datum>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<Datum> ctx) throws Exception {
                while (isRunning) {
                    Thread.sleep((int) (Math.random() * 10));
                    Datum address = new Datum("a", (int) (Math.random() * 100));
                    ctx.collect(address);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }, TypeInformation.of(Datum.class));


        StreamingFileSink sink = StreamingFileSink.forBulkFormat(new Path("hdfs://localhost/flink_data/a"),
                ParquetAvroWriters.forReflectRecord(Datum.class))
                .build();


        ds.addSink(sink);
        ds.print();
        env.execute();


    }


    /**
     * Test datum.
     */
    public static class Datum implements Serializable {

        public String a;
        public int b;

        public Datum() {
        }

        public Datum(String a, int b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Datum datum = (Datum) o;
            return b == datum.b && (a != null ? a.equals(datum.a) : datum.a == null);
        }

        @Override
        public int hashCode() {
            int result = a != null ? a.hashCode() : 0;
            result = 31 * result + b;
            return result;
        }
    }
}
