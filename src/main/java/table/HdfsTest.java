/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package table;

import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * Simple example for demonstrating the use of SQL on a Stream Table in Java.
 *
 * <p>This example shows how to:
 * - Convert DataStreams to Tables
 * - Register a Table under a name
 * - Run a StreamSQL query on the registered Table
 */
public class HdfsTest {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));

        tEnv.registerDataStream("OrderB", orderB, "user, product, amount");
        Table result =  tEnv.sqlQuery("SELECT * FROM OrderB");
//        tEnv.toAppendStream(result, Row.class).print();

//        BucketingSink<Row> sink = new BucketingSink<>("hdfs:///flink-data");
//        sink.setBucketer(new DateTimeBucketer<Row>("yyyy-MM-dd", ZoneId.of("UTC+8")));
//        sink.setWriter(new SequenceFileWriter());
//        sink.setBatchSize(1024 * 1024 * 10); // this is 10 MB,
//        sink.setBatchRolloverInterval(1 * 60 * 1000); // this is 3 mins



//
//        StreamingFileSink<Order> sink = StreamingFileSink
//                .forRowFormat(new Path("hdfs:///flink-data/a"), new SimpleStringEncoder<Order>("UTF-8"))
//                .build();




        StreamingFileSink<Order> sink = StreamingFileSink.forBulkFormat(
                new Path("file:///tmp/flink-data/a"),
                ParquetAvroWriters.forReflectRecord(Order.class))
                .build();


        tEnv.toAppendStream(result,Order.class).addSink(sink);
		env.execute();

    }

    // *************************************************************************
    //     USER DATA TYPES
    // *************************************************************************

    /**
     * Simple POJO.
     */
    public static class Order implements  java.io.Serializable{
        public Long user;
        public String product;
        public int amount;

        public Order() {
        }

        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }


    }
}
