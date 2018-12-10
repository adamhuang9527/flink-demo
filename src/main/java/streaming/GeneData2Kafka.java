package streaming;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

/**
 * write file data to kafka
 * 
 * @author user
 *
 */
public class GeneData2Kafka {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final String province[] = new String[] { "上海", "云南", "内蒙", "北京", "吉林", "四川", "国外", "天津", "宁夏", "安徽", "山东", "山西",
				"广东", "广西", "江苏", "江西", "河北", "河南", "浙江", "海南", "湖北", "湖南", "甘肃", "福建", "贵州", "辽宁", "重庆", "陕西", "香港",
				"黑龙江" };

		DataStream<String> source = env.addSource(new SourceFunction<String>() {

			private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			private volatile boolean isRunning = true;
			private static final long serialVersionUID = 1L;
			long count = 0;
			private Date date;
			private AtomicLong al = new AtomicLong(0L);

			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				while (isRunning) {
					Thread.sleep(10);
					date = new Date();
					StringBuffer ss = new StringBuffer();
					String pro = province[(int) (Math.random() * 29)];
					ss.append(pro).append("\t");
					String id = "id-" + (int) (Math.random() * 1000);
					ss.append(id).append("\t");
					ss.append(date.getTime()).append("\t");
					ss.append(sdf.format(date)).append("\t");
					ss.append(al.incrementAndGet());
					ctx.collect(ss.toString());
				}
			}

			@Override
			public void cancel() {
				isRunning = false;
			}
		});

//		source.print();

		FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>("localhost:9092", // broker list
				args[0], // target topic
				new SimpleStringSchema()); // serialization schema
		source.addSink(myProducer);
		env.execute("gene data to kafka");
	}
}
