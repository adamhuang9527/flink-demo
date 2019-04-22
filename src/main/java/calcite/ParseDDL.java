package calcite;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.junit.Test;

public class ParseDDL {
    @Test
    public void test1() throws SqlParseException {
        String sql = "CREATE TABLE tt_stream(\n" +
                "  a VARCHAR,\n" +
                "  b VARCHAR,\n" +
                "  c TIMESTAMP,\n" +
                "  WATERMARK wk1 FOR c as withOffset(c, 1000)  --watermark计算方法\n" +
                ") WITH (\n" +
                "  type = 'sls1',\n" +
                "  topic = 'yourTopicName',\n" +
                "  accessId = 'yourAccessId',\n" +
                "  accessKey = 'yourAccessSecret'\n" +
                ")";
        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(FlinkSqlParserImpl.FACTORY)
                .setLex(Lex.JAVA)
                .build();
        SqlParser parser = SqlParser.create(sql, config);
        SqlNode node = parser.parseStmt();
        System.out.println(node);
    }

    @Test
    public void test2() throws SqlParseException {
        String sql = "CREATE TABLE tt_stream(\n" +
                "  a VARCHAR,\n" +
                "  b VARCHAR,\n" +
                "  c TIMESTAMP " +
                ") ";
        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(SqlDdlParserImpl.FACTORY)
                .setLex(Lex.JAVA)
                .build();
        SqlParser parser = SqlParser.create(sql, config);
        SqlNode node = parser.parseStmt();
        System.out.println(node);
    }

}
