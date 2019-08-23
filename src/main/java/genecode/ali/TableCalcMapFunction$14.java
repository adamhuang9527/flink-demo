//package genecode.ali;
//
//public class TableCalcMapFunction$14
//        extends org.apache.flink.api.common.functions.RichFlatMapFunction {
//
//    final org.apache.flink.table.dataformat.GenericRow out = new org.apache.flink.table.dataformat.GenericRow(2);
//
//    public TableCalcMapFunction$14(Object[] references) throws Exception {
//
//    }
//
//
//    @Override
//    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
//
//    }
//
//    @Override
//    public void flatMap(Object _in1, org.apache.flink.util.Collector c) throws Exception {
//        org.apache.flink.table.dataformat.BaseRow in1 = (org.apache.flink.table.dataformat.BaseRow) _in1;
//
//
//        if (in1.isNullAt(2)) {
//            out.setNullAt(1);
//        } else {
//            out.setField(1, in1.getString(2));
//            ;
//        }
//
//
//        if (in1.isNullAt(1)) {
//            out.setNullAt(0);
//        } else {
//            out.setField(0, in1.getLong(1));
//            ;
//        }
//
//
//        c.collect(out);
//
//
//    }
//
//    @Override
//    public void close() throws Exception {
//
//    }
//}
//