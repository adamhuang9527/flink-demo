package avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TestAvro {
    DataFileWriter<Address> fileWriter;
    @Before
    public void init() throws IOException {
        DatumWriter<Address> writer = new SpecificDatumWriter<>();
        fileWriter = new DataFileWriter<>(writer);
        fileWriter.create(Address.getClassSchema(), new File("/tmp/Address.avro"));

    }
    @Test
    public void testProcAvro() throws IOException {


        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");


        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        //生产者发送消息
        String topic = "kafkaSourceAvro";
        Producer<byte[], byte[]> procuder = new KafkaProducer<>(props);


        for (int i = 1; i <= 1000000; i++) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (i % 100 == 0) {
                System.out.println("i is " + i);
            }

            Address address = new Address();

            address.setNum(i);
            address.setCity("city" + i);
            address.setState("state");
            address.setZip("zip001");
            address.setStreet("street");


            byte[]  result = serializeUser(address);

            ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(topic, ("key" + i).getBytes(), result);
            procuder.send(msg);
        }

        System.out.println("send message over.");
        procuder.close(100, TimeUnit.MILLISECONDS);

    }


    @Test
    public void testDeserializeAvro() throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("kafkaSinkAvro"));
        while (true) {
            ConsumerRecords<byte[], byte[]> records = null;
            try {
                records = consumer.poll(1000);
            } catch (Exception e) {
                System.out.println("-----------------------");
                e.printStackTrace();
            }

            for (ConsumerRecord<byte[], byte[]> record : records) {
                Address address = deserializeUser(record.value());
                System.out.println(address.toString());
//
//					String data = String.format("offset = %d, key = %s, value = %s", record.offset(), record.key(),
////						record.value());
//					System.out.println(data);


            }
        }
    }

    @Test
    public void testWriteFile() throws IOException {


        Address address = new Address();
        address.setNum(1);
        address.setCity("city");
        address.setState("state");
        address.setZip("zip001");
        address.setStreet("street");

        String path = "/tmp/aaa.avro"; // avro文件存放目录
        DatumWriter<Address> userDatumWriter = new SpecificDatumWriter<>(Address.class);
        DataFileWriter<Address> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        dataFileWriter.create(address.getSchema(),  new File(path));
        dataFileWriter.append(address);
        dataFileWriter.close();
    }

    @Test
    public void testWriteFileHdfs() throws IOException {
        Configuration conf = new Configuration();
        Path dstPath = new Path("file:///tmp/abc.avro");
        FileSystem fs = dstPath.getFileSystem(conf);
        FSDataOutputStream outputStream = fs.create(dstPath);


        Address address = new Address();
        address.setNum(1);
        address.setCity("city" );
        address.setState("state");
        address.setZip("zip001");
        address.setStreet("street");

        outputStream.write(serializeUser(address));
        outputStream.close();
        fs.close();
    }

    @Test
    public void readFromFile() throws IOException {
        DatumReader<Address> reader = new SpecificDatumReader<>();
        DataFileReader<Address> dataFileReader = new DataFileReader<>(new File("/tmp/flink-data/avro/2019-05-29--10/.part-1-0.inprogress.562a3959-1db8-4a19-98ac-b17cf8f2d3fa"), reader);
        Address user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next();
            System.out.println(user);
        }
        dataFileReader.close();
    }


    @Test
    public void test() throws IOException {
        Address address = new Address();

        address.setNum(1);
        address.setCity("city" );
        address.setState("state");
        address.setZip("zip001");
        address.setStreet("street");
        Address deserializedUser = deserializeUser(serializeUser(address));

        System.out.println(deserializedUser.getNum());
    }

    private  byte[] serializeUser(Address user) throws IOException {
        DatumWriter<Address> userDatumWriter = new SpecificDatumWriter<>(Address.class);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
        userDatumWriter.write(user, binaryEncoder);
        return outputStream.toByteArray();
    }


    private Address deserializeUser(byte[] data) throws IOException {
        DatumReader<Address> userDatumReader = new SpecificDatumReader<>(Address.class);
        BinaryDecoder binaryEncoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(data), null);
        return userDatumReader.read(new Address(), binaryEncoder);
    }
}
