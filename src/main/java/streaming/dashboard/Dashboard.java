package streaming.dashboard;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.time.ZoneId;


/**
 * 从流读取数据，映射成table，然后sql查询，将查询结果发送到本地文件，hdfs，和控制台
 *
 * sql:按照一秒的时间窗口计算pv,平均响应时间,错误率（status不等于200的占比）
 */
public class Dashboard {
    public static void main(String[] args) throws Exception {


        // traceid,userid,timestamp,status,response time
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

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


        StreamTableEnvironment tenv = TableEnvironment.getTableEnvironment(env);
        tenv.registerDataStream("log", ds, "traceid,userid,timestamp,status,restime,proctime.proctime,rowtime.rowtime");


        String sql = "select pv,avg_res_time,round(CAST(errorcount AS DOUBLE)/pv,2) as errorRate,(starttime + interval '8' hour ) as stime from (select count(*) as pv,AVG(restime) as avg_res_time  ," +
                "sum(case when status = 200 then 0 else 1 end) as errorcount, " +
                "TUMBLE_START(rowtime,INTERVAL '1' SECOND)  as starttime " +
                "from log where status <> 404 group by TUMBLE(rowtime,INTERVAL '1' SECOND) )";

//       String sql = "select case when status = 200 then 0 else 1 end as errorcount from log";
        Table result1 = tenv.sqlQuery(sql);


        //write to csv sink

//        TableSink csvSink = new CsvTableSink("/Users/user/work/flink_data/log", "|");
//        String[] fieldNames = {"a", "b", "c", "d"};
//        TypeInformation[] fieldTypes = {Types.LONG, Types.INT, Types.DOUBLE, Types.SQL_TIMESTAMP};
//        tenv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink);
//        result1.insertInto("CsvSinkTable");

        //write to hdfs sink
        BucketingSink<Result> sink = new BucketingSink<Result>("/Users/user/work/flink_data");
        sink.setUseTruncate(false);


//        sink.setBucketer(new Bucketer<UI>() {
//            @Override
//            public Path getBucketPath(Clock clock, Path basePath, UI element) {
//                String newDateTimeString = dateTimeFormatter.format(Instant.ofEpochMilli(element.getTimestamp()));
//                return new Path(basePath + "/" + newDateTimeString);
//            }
//        });
        sink.setBucketer(new DateTimeBucketer<Result>("yyyy-MM-dd--HHmm", ZoneId.of("UTC+8")));
        sink.setWriter(new StringWriter<Result>());
        sink.setBatchSize(1024 * 1024 * 10); // this is 10 MB,
        sink.setBatchRolloverInterval(60 * 1000); // this is 1 min

        tenv.toAppendStream(result1, Result.class).addSink(sink);

        //输出到控制台
        tenv.toAppendStream(result1, Row.class).print();


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
