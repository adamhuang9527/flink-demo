package streaming.jdbc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.Collections;

public class JDBCSinkTest {

    private static final String[] FIELD_NAMES = new String[]{"foo"};
    private static final TypeInformation[] FIELD_TYPES = new TypeInformation[]{
            BasicTypeInfo.STRING_TYPE_INFO
    };
    private static final RowTypeInfo ROW_TYPE = new RowTypeInfo(FIELD_TYPES, FIELD_NAMES);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> ds = env.fromCollection(Collections.singleton(Row.of("foo")), ROW_TYPE);
        JDBCAppendTableSink jdbcAppendTableSink = JDBCAppendTableSink.builder()
                .setDrivername("foo")
                .setDBUrl("bar")
                .setQuery("insert into %s (id) values (?)")
                .setParameterTypes(FIELD_TYPES)
                .build();

        jdbcAppendTableSink.emitDataStream(ds);
        env.execute();
    }
}
