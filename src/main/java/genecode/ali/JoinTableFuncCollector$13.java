//package genecode.ali;
//
//public class JoinTableFuncCollector$13 extends org.apache.flink.table.runtime.collector.TableFunctionCollector {
//
//    final org.apache.flink.table.dataformat.GenericRow out = new org.apache.flink.table.dataformat.GenericRow(2);
//    final org.apache.flink.table.dataformat.JoinedRow joinedRow$12 = new org.apache.flink.table.dataformat.JoinedRow();
//
//    public JoinTableFuncCollector$13(Object[] references) throws Exception {
//
//    }
//
//    @Override
//    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
//
//    }
//
//    @Override
//    public void collect(Object record) throws Exception {
//        org.apache.flink.table.dataformat.BaseRow in1 = (org.apache.flink.table.dataformat.BaseRow) getInput();
//        org.apache.flink.table.dataformat.BaseRow in2 = (org.apache.flink.table.dataformat.BaseRow) record;
//        long field$10;
//        boolean isNull$10;
//        org.apache.flink.table.dataformat.BinaryString field$11;
//        boolean isNull$11;
//        isNull$11 = in2.isNullAt(1);
//        field$11 = org.apache.flink.table.dataformat.BinaryString.EMPTY_UTF8;
//        if (!isNull$11) {
//            field$11 = in2.getString(1);
//        }
//        isNull$10 = in2.isNullAt(0);
//        field$10 = -1L;
//        if (!isNull$10) {
//            field$10 = in2.getLong(0);
//        }
//
//
//        if (isNull$10) {
//            out.setNullAt(0);
//        } else {
//            out.setField(0, field$10);
//            ;
//        }
//
//
//        if (isNull$11) {
//            out.setNullAt(1);
//        } else {
//            out.setField(1, field$11);
//            ;
//        }
//
//
//        joinedRow$12.replace(in1, out);
//        joinedRow$12.setHeader(in1.getHeader());
//        outputResult(joinedRow$12);
//
//    }
//
//    @Override
//    public void close() throws Exception {
//
//    }
//}
//