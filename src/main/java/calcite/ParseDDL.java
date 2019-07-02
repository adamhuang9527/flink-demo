package calcite;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.junit.Test;

public class ParseDDL {
//    @Test
//    public void test1() throws SqlParseException {
//        String sql = "CREATE  TABLE tt_stream(\n" +
//                "  a VARCHAR,\n" +
//                "  b VARCHAR,\n" +
//                "  c TIMESTAMP,\n" +
//                "  WATERMARK wk1 FOR c as withOffset(c, 1000)  --watermark计算方法\n" +
//                ") WITH (\n" +
//                "  type = 'sls1',\n" +
//                "  topic = 'yourTopicName',\n" +
//                "  accessId = 'yourAccessId',\n" +
//                "  accessKey = 'yourAccessSecret'\n" +
//                ")";
//        String sql1 ="CREATE TABLE Products (\n" +
//                "  productId VARCHAR,        -- 产品 id\n" +
//                "  name VARCHAR,             -- 产品名称\n" +
//                "  unitPrice DOUBLE ,         -- 单价\n" +
//                "  PERIOD FOR SYSTEM_TIME,   -- 这是一张随系统时间而变化的表，用来声明维表\n" +
//                "  PRIMARY KEY (productId)   -- 维表必须声明主键\n" +
//                ") with (\n" +
//                "  type = 'alihbase'       -- HBase 数据源\n" +
//                ")";
//        SqlParser.Config config = SqlParser.configBuilder()
//                .setParserFactory(FlinkSqlParserImpl.FACTORY)
//                .setLex(Lex.JAVA)
//                .build();
//        SqlParser parser = SqlParser.create(sql1, config);
//        SqlNode node = parser.parseStmt();
//        System.out.println(node);
//    }
//
//    @Test
//    public void test2() throws SqlParseException {
//        String sql = "CREATE TABLE tt_stream(\n" +
//                "  a VARCHAR,\n" +
//                "  b VARCHAR,\n" +
//                "  c TIMESTAMP " +
//                ") ";
//        SqlParser.Config config = SqlParser.configBuilder()
//                .setParserFactory(SqlDdlParserImpl.FACTORY)
//                .setLex(Lex.JAVA)
//                .build();
//        SqlParser parser = SqlParser.create(sql, config);
//        SqlNode node = parser.parseStmt();
//        System.out.println(node);
//    }
//
//

//    /**
//     * 测试维表，通过阿里的sql解析
//     * @throws SqlParseException
//     */
//    @Test
//    public void testPeriod() throws SqlParseException {
//        String sql = "select * from Orders as o join RatesHistory FOR SYSTEM_TIME AS OF PROCTIME() as r on r.currency = o.currency";
//        SqlParser.Config config = SqlParser.configBuilder()
//                .setParserFactory(FlinkSqlParserImpl.FACTORY)
//                .setLex(Lex.JAVA)
//                .build();
//        SqlParser parser = SqlParser.create(sql, config);
//        SqlNode node = parser.parseStmt();
//        System.out.println(node);
//    }



    @Test
    public void testSideSelect() throws SqlParseException {
        String sql = "select * from Orders as o join RatesHistory FOR SYSTEM_TIME AS OF PROCTIME() as r on r.currency = o.currency";
        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(SqlParserImpl.FACTORY)
                .setLex(Lex.MYSQL)
                .build();
        SqlParser parser = SqlParser.create(sql, config);

        SqlNode node = parser.parseStmt();
        System.out.println(node);


//        VolcanoPlanner planner = new VolcanoPlanner();
//        planner.v
//        RexBuilder rexBuilder = createRexBuilder();
//        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
//        SqlToRelConverter converter = new SqlToRelConverter(...);
//        RelRoot root = converter.convertQuery(node, false, true);
    }


//    /**
//     * 测试维表，通过flink的sql解析
//     * @throws SqlParseException
//     */
//    @Test
//    public void testPeriorByFlink()  {
//        String sql = "select * from Orders as o join RatesHistory FOR SYSTEM_TIME AS OF PROCTIME() as r on r.currency = o.currency";
//
//
//        String sql1 ="CREATE TABLE Products (\n" +
//                "  productId VARCHAR,        -- 产品 id\n" +
//                "  name VARCHAR,             -- 产品名称\n" +
//                "  unitPrice DOUBLE ,         -- 单价\n" +
//                "  PERIOD FOR SYSTEM_TIME,   -- 这是一张随系统时间而变化的表，用来声明维表\n" +
//                "  PRIMARY KEY (productId)   -- 维表必须声明主键\n" +
//                ") with (\n" +
//                "  type = 'alihbase'       -- HBase 数据源\n" +
//                ")";
//
//        SqlParser.Config config = SqlParser.configBuilder()
//                .setParserFactory(SqlDdlParserImpl.FACTORY)
//                .setLex(Lex.JAVA)
//                .build();
//        SqlParser parser = SqlParser.create(sql1, config);
//        SqlNode node = parser.parseStmt();
//
//        System.out.println(node);
//    }

}
