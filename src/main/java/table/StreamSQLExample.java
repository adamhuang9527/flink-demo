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

import org.apache.avro.util.Utf8;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import java.util.Arrays;

/**
 * Simple example for demonstrating the use of SQL on a Stream Table in Java.
 *
 * <p>This example shows how to:
 * - Convert DataStreams to Tables
 * - Register a Table under a name
 * - Run a StreamSQL query on the registered Table
 */
public class StreamSQLExample {


    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));

        Table tableA = tEnv.fromDataStream(orderA, "user, product, amount");


        DataStream<Address> orderB = env.fromCollection(Arrays.asList(
                new Address(2, "aaaaaa", "b", "c", "d")));


        // refilterByFactoryClassgister DataStream as TabltEnve
        tEnv.registerDataStream("OrderB", orderB, "num, street, city,state,zip");


        char c = '|';
        tEnv.connect(new Kafka()
                .version("0.10")
                .topic("kafkaSink")
                .property("bootstrap.servers", "localhost:9092"))
                .withFormat(new Avro().recordClass(Address.class))
//                .withFormat(new Csv().fieldDelimiter(c).deriveSchema())
//                .withFormat(new Json().failOnMissingField(false).deriveSchema())
                .withSchema(new Schema().field("a", Types.INT())
                        .field("b", org.apache.flink.api.common.typeinfo.Types.GENERIC(Utf8.class))
                        .field("c", org.apache.flink.api.common.typeinfo.Types.GENERIC(Utf8.class))
                        .field("d", org.apache.flink.api.common.typeinfo.Types.GENERIC(Utf8.class))
                        .field("e", org.apache.flink.api.common.typeinfo.Types.GENERIC(Utf8.class))
                )
                .inAppendMode()
                .registerTableSink("mysink");


//        tEnv.connect(new FileSystem().path("file:///tmp/flink-data/"))
//                .withFormat(new OldCsv().fieldDelimiter("|")
//                        .field("appName", Types.LONG())
//                        .field("clientIp", Types.STRING())
//                        .field("rowtime", Types.INT()))
//                .withSchema(new Schema()
//                        .field("appName", Types.LONG())
//                        .field("clientIp", Types.STRING())
//                        .field("rowtime", Types.INT()))
//                .inAppendMode()
//                .registerTableSink("mysink");


//        Table result =  tEnv.sqlQuery("SELECT * FROM OrderB");
//
        tEnv.sqlUpdate("insert into mysink SELECT * FROM OrderB");

//
//        Table result = tEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL " +
//                "SELECT * FROM OrderB WHERE amount < 2");
////        result.insertInto("mysink");
//
//        DataStream r =  tEnv.toAppendStream(result, Address.class);
//        r.print();


        System.out.println(env.getExecutionPlan());
//		env.execute();

    }

    // *************************************************************************
    //     USER DATA TYPES
    // *************************************************************************

    /**
     * Simple POJO.
     */
    public static class Order {
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


        public Long getUser() {
            return user;
        }

        public void setUser(Long user) {
            this.user = user;
        }

        public String getProduct() {
            return product;
        }

        public void setProduct(String product) {
            this.product = product;
        }

        public int getAmount() {
            return amount;
        }

        public void setAmount(int amount) {
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
