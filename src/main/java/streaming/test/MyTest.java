package streaming.test;

import org.apache.flink.types.Row;

public class MyTest {

	public static void main(String[] args) {
		Row row = new Row(2);
		row.setField(0,"111");
		System.out.println(row);
	}

}
