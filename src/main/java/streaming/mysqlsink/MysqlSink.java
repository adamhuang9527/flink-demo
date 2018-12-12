package streaming.mysqlsink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class MysqlSink extends RichSinkFunction<List> {

	private static final long serialVersionUID = 1L;

	private Connection connection;
	private PreparedStatement preparedStatement;

	private String username = "";
	private String password;
	private String url;
	private String sql;
	private List params;

	public MysqlSink(String username, String password, String url, String sql) {
		super();
		this.username = username;
		this.password = password;
		this.url = url;
		this.sql = sql;
	}

	/**
	 * open方法是初始化方法，会在invoke方法之前执行，执行一次。
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		String DRIVERNAME = "com.mysql.jdbc.Driver";
		// 加载JDBC驱动
		Class.forName(DRIVERNAME);
		// 获取数据库连接
		connection = DriverManager.getConnection(url, username, password);
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
	public void invoke(List data) throws Exception {
		try {
			if (null != data) {
				for (int i = 0; i < data.size(); i++) {
					preparedStatement.setObject(i + 1, data.get(i));
				}
			}
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
