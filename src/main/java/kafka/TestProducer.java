package kafka;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TestProducer {


    @Test
    public void testProdJson() {

        Properties props = new Properties();
//        props.put("bootstrap.servers", "10.160.85.185:9092,10.160.85.185:9093,10.160.85.185:9094");
        props.put("bootstrap.servers", "localhost:9092");


        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //生产者发送消息
        String topic = "test3";
        Producer<String, String> procuder = new KafkaProducer<String, String>(props);
        for (int i = 1; i <= 100000; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (i % 100 == 0) {
                System.out.println("i is " + i);
//                String s="aaaaaaaa";
//                ProducerRecord<String, String> msg = new ProducerRecord<>(topic,s);
//                procuder.send(msg);
            }


            JSONObject json = new JSONObject();
            json.put("appName", "name");
            json.put("clientIp", "value" + i);
            json.put("uploadTime", System.currentTimeMillis());
//            Date date  = new Date();
//            json.put("mytimestamp",new  SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").format(date));
//            json.put("mydate",new  SimpleDateFormat("yyyy-MM-dd").format(date));
//            JSONObject jsonChild = new JSONObject();
//            jsonChild.put("price", 100);
//            jsonChild.put("timeStamp", 1559640229126L);
//
//            json.put("charge", jsonChild);

            ProducerRecord<String, String> msg = new ProducerRecord<>(topic, "value" + i, json.toJSONString());
            procuder.send(msg);
        }

        System.out.println("send message over.");
        procuder.close(100, TimeUnit.MILLISECONDS);
    }


    @Test
    public void testProdJsonAddress() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");


        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //生产者发送消息
        String topic = "kafkaSourceJson";
        Producer<String, String> procuder = new KafkaProducer<String, String>(props);
        for (int i = 1; i <= 10; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (i % 100 == 0) {
                System.out.println("i is " + i);
            }
            JSONObject json = new JSONObject();
            json.put("num", i);
            json.put("city", "city" + i);
            json.put("state", "state" + i);
            json.put("zip", "zip" + i);
            json.put("street", "street" + i);


            ProducerRecord<String, String> msg = new ProducerRecord<>(topic, "key" + i, json.toJSONString());
            procuder.send(msg);
        }

        System.out.println("send message over.");
        procuder.close(100, TimeUnit.MILLISECONDS);
    }


//    @Test
//    public void testProcAvro() throws IOException {
//
//
//        Properties props = new Properties();
////        props.put("bootstrap.servers", "10.160.85.185:9092,10.160.85.185:9093,10.160.85.185:9094");
//        props.put("bootstrap.servers", "localhost:9092");
//
//
//        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
//        //生产者发送消息
//        String topic = "kafkaSourceAvro";
//        Producer<byte[], byte[]> procuder = new KafkaProducer<>(props);
//        byte[] result = null;
//        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//        for (int i = 1; i <= 100000; i++) {
//            try {
//                Thread.sleep(100);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            if (i % 100 == 0) {
//                System.out.println("i is " + i);
//            }
//
//            Address address = new Address();
//            DatumWriter<Address> datumWrite = new GenericDatumWriter<>(address.getSchema());
//
//            DataFileWriter<Address> dataFileWriter = new DataFileWriter<>(datumWrite);
//            dataFileWriter.create(address.getSchema(), outputStream);
//            dataFileWriter.append(address);
//            result = outputStream.toByteArray();
//
//            ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(topic, ("key" + i).getBytes(), ("value" + i).getBytes());
//            procuder.send(msg);
//        }
//
//        System.out.println("send message over.");
//        procuder.close(100, TimeUnit.MILLISECONDS);
//
//    }
}