package streaming;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

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

		source.print();

//		MysqlSink sink = new MysqlSink("test", "test", "jdbc:mysql://10.160.85.183:3306/test",
//				"insert into test.userinfo values(?,?,?,?,?)");
//
//		source.addSink(sink);
		env.execute("gene data to mysql");
	}


}
