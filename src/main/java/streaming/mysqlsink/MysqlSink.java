package streaming.mysqlsink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class MysqlSink extends RichSinkFunction<Tuple2<String, Long>> {

	private static final long serialVersionUID = 1L;

	private Connection connection;
	private PreparedStatement preparedStatement;

	/**
	 * open方法是初始化方法，会在invoke方法之前执行，执行一次。
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		// JDBC连接信息
		String USERNAME = "root";
		String PASSWORD = "root";
		String DRIVERNAME = "com.mysql.jdbc.Driver";
		String DBURL = "jdbc:mysql://localhost:3306/flink";
		// 加载JDBC驱动
		Class.forName(DRIVERNAME);
		// 获取数据库连接
		connection = DriverManager.getConnection(DBURL, USERNAME, PASSWORD);
		String sql = "insert into flink.ui1(id,time) values (?,?)";
		preparedStatement = connection.prepareStatement(sql);
		super.open(parameters);
	}

	/**
	 * invoke()方法解析一个元组数据，并插入到数据库中。
	 * 
	 * @param data 输入的数据
	 * @throws Exception
	 */
	@Override
	public void invoke(Tuple2<String, Long> data) throws Exception {
		try {
			preparedStatement.setString(1, data.f0);
			preparedStatement.setLong(2, data.f1);
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
		}

	};

	/**
	 * close()是tear down的方法，在销毁时执行，关闭连接。
	 */
	@Override
	public void close() throws Exception {
		if (preparedStatement != null) {
			preparedStatement.close();
		}
		if (connection != null) {
			connection.close();
		}
		super.close();
	}
}
