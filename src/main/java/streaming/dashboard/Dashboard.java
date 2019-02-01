package streaming.dashboard;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.time.ZoneId;


/**
 * side output 、table、sql、多sink的使用
 *
 * 功能
 * 1。source从流接收数据，从主流数据中引出side outout进行计算，消费同一个流，避免多次接入，消耗网略
 * 2.通过table和sql计算pv,平均响应时间,错误率（status不等于200的占比）
 * 3。原始数据输出到hdfs。
 * 4，side output的结果数据发送到csv sink
 */
public class Dashboard {
    public static void main(String[] args) throws Exception {


        // traceid,userid,timestamp,status,response time
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple5<String, Integer, Long, Integer, Integer>> ds = env.addSource(new GeneData()).
                flatMap((String value, Collector<Tuple5<String, Integer, Long, Integer, Integer>> out) -> {

                    String ss[] = value.split(",");

                    out.collect(Tuple5.of(ss[0], Integer.parseInt(ss[1]), Long.parseLong(ss[2]), Integer.parseInt(ss[3]), Integer.parseInt(ss[4])));
                }).returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG, Types.INT, Types.INT))
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple5<String, Integer, Long, Integer, Integer>>() {
                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(Tuple5<String, Integer, Long, Integer, Integer> lastElement, long extractedTimestamp) {
                        return new Watermark(lastElement.f2);
                    }

                    @Override
                    public long extractTimestamp(Tuple5<String, Integer, Long, Integer, Integer> element, long previousElementTimestamp) {
                        return element.f2;
                    }
                });


        final OutputTag<Tuple5<String, Integer, Long, Integer, Integer>> outputTag = new OutputTag<Tuple5<String, Integer, Long, Integer, Integer>>("side-output"){};


        SingleOutputStreamOperator<Tuple5<String, Integer, Long, Integer, Integer>> mainDataStream =  ds.process(new ProcessFunction<Tuple5<String, Integer, Long, Integer, Integer>, Tuple5<String, Integer, Long, Integer, Integer>>() {
            @Override
            public void processElement(Tuple5<String, Integer, Long, Integer, Integer> value, Context ctx, Collector<Tuple5<String, Integer, Long, Integer, Integer>> out) throws Exception {
                ctx.output(outputTag,value);
                out.collect(value);
            }
        });




        DataStream<Tuple5<String, Integer, Long, Integer, Integer>>  dataStream = mainDataStream.getSideOutput(outputTag);


        StreamTableEnvironment tenv = TableEnvironment.getTableEnvironment(env);
        tenv.registerDataStream("log", dataStream, "traceid,userid,timestamp,status,restime,proctime.proctime,rowtime.rowtime");


        String sql = "select pv,avg_res_time,round(CAST(errorcount AS DOUBLE)/pv,2) as errorRate,(starttime + interval '8' hour ) as stime from (select count(*) as pv,AVG(restime) as avg_res_time  ," +
                "sum(case when status = 200 then 0 else 1 end) as errorcount, " +
                "TUMBLE_START(rowtime,INTERVAL '1' SECOND)  as starttime " +
                "from log where status <> 404 group by TUMBLE(rowtime,INTERVAL '1' SECOND) )";

        Table result1 = tenv.sqlQuery(sql);


        //write to csv sink

        TableSink csvSink = new CsvTableSink("/Users/user/work/flink_data/log", "|");
        String[] fieldNames = {"a", "b", "c", "d"};
        TypeInformation[] fieldTypes = {Types.LONG, Types.INT, Types.DOUBLE, Types.SQL_TIMESTAMP};
        tenv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink);
        result1.insertInto("CsvSinkTable");





        //write to hdfs sink
        BucketingSink<Tuple5<String, Integer, Long, Integer, Integer>> sink = new BucketingSink<>("hdfs://localhost/logs/");
        sink.setUseTruncate(false);
//        sink.setBucketer(new Bucketer<UI>() {
//            @Override
//            public Path getBucketPath(Clock clock, Path basePath, UI element) {
//                String newDateTimeString = dateTimeFormatter.format(Instant.ofEpochMilli(element.getTimestamp()));
//                return new Path(basePath + "/" + newDateTimeString);
//            }
//        });
        sink.setBucketer(new DateTimeBucketer<Tuple5<String, Integer, Long, Integer, Integer>>("yyyy-MM-dd--HHmm", ZoneId.of("UTC+8")));
        sink.setWriter(new StringWriter<Tuple5<String, Integer, Long, Integer, Integer>>());
        sink.setBatchSize(1024 * 1024 * 10); // this is 10 MB,
        sink.setBatchRolloverInterval(60 * 1000); // this is 1 min
        ds.addSink(sink);



//        tenv.toAppendStream(result1, Result.class).addSink(sink);

        //输出到控制台
//        tenv.toAppendStream(result1, Row.class).print();

        tenv.toRetractStream()


        env.execute();

    }

    public static class Result {
        private Long pv;
        private Integer avg_res_time;
        private Double errorRate;
        private Timestamp stime;


        public Long getPv() {
            return pv;
        }

        public void setPv(Long pv) {
            this.pv = pv;
        }

        public Integer getAvg_res_time() {
            return avg_res_time;
        }

        public void setAvg_res_time(Integer avg_res_time) {
            this.avg_res_time = avg_res_time;
        }

        public Double getErrorRate() {
            return errorRate;
        }

        public void setErrorRate(Double errorRate) {
            this.errorRate = errorRate;
        }

        public Timestamp getStime() {
            return stime;
        }

        public void setStime(Timestamp stime) {
            this.stime = stime;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "pv=" + pv +
                    ", avg_res_time=" + avg_res_time +
                    ", errorRate=" + errorRate +
                    ", stime=" + stime +
                    '}';
        }
    }

}
