package org.apache.flink.table.examples.java;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class Test1 {
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<String> ds = env.fromElements(null);
		
		
		
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		
	}
}
