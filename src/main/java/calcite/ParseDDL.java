package calcite;

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
//        SqlParser.Config config = SqlParser.configBuilder()
//                .setParserFactory(FlinkSqlParserImpl.FACTORY)
//                .setLex(Lex.JAVA)
//                .build();
//        SqlParser parser = SqlParser.create(sql, config);
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
//    @Test
//    public void test3() throws SqlParseException {
//        String sql = "insert into table1,table2 select * from table2 ";
//        SqlParser.Config config = SqlParser.configBuilder()
//                .setParserFactory(SqlDdlParserImpl.FACTORY)
//                .setLex(Lex.JAVA)
//                .build();
//        SqlParser parser = SqlParser.create(sql, config);
//        SqlNode node = parser.parseStmt();
//        System.out.println(node);
//    }

}
