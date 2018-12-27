package streaming;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import streaming.mysqlsink.MysqlSink;

/**
 * write file data to kafka
 * 
 * @author user
 *
 */
public class GeneData2Mysql {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<List> source = env.addSource(new GeneUISource());

//		source.print();

		MysqlSink sink = new MysqlSink("test", "test", "jdbc:mysql://10.160.85.183:3306/test",
				"insert into test.userinfo values(?,?,?,?,?)");

		source.addSink(sink);
		env.execute("gene data to mysql");
	}


}
