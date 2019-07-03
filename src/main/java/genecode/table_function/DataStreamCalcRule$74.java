package genecode.table_function;

public class DataStreamCalcRule$74 extends org.apache.flink.streaming.api.functions.ProcessFunction {

  
  final org.apache.flink.types.Row out =
      new org.apache.flink.types.Row(10);
  
  

  public DataStreamCalcRule$74() throws Exception {
    
    
  }

  
  

  @Override
  public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
    
    
  }

  @Override
  public void processElement(Object _in1, org.apache.flink.streaming.api.functions.ProcessFunction.Context ctx, org.apache.flink.util.Collector c) throws Exception {
    org.apache.flink.types.Row in1 = (org.apache.flink.types.Row) _in1;
    
    boolean isNull$67 = (java.lang.Integer) in1.getField(6) == null;
    int result$66;
    if (isNull$67) {
      result$66 = -1;
    }
    else {
      result$66 = (java.lang.Integer) in1.getField(6);
    }
    
    
    boolean isNull$65 = (java.lang.String) in1.getField(5) == null;
    java.lang.String result$64;
    if (isNull$65) {
      result$64 = "";
    }
    else {
      result$64 = (java.lang.String) (java.lang.String) in1.getField(5);
    }
    
    
    boolean isNull$59 = (java.lang.Long) in1.getField(2) == null;
    long result$58;
    if (isNull$59) {
      result$58 = -1L;
    }
    else {
      result$58 = (java.lang.Long) in1.getField(2);
    }
    
    
    boolean isNull$55 = (java.lang.String) in1.getField(0) == null;
    java.lang.String result$54;
    if (isNull$55) {
      result$54 = "";
    }
    else {
      result$54 = (java.lang.String) (java.lang.String) in1.getField(0);
    }
    
    
    boolean isNull$63 = (java.lang.Long) in1.getField(4) == null;
    long result$62;
    if (isNull$63) {
      result$62 = -1L;
    }
    else {
      result$62 = (java.lang.Long) in1.getField(4);
    }
    
    
    boolean isNull$71 = (java.lang.String) in1.getField(8) == null;
    java.lang.String result$70;
    if (isNull$71) {
      result$70 = "";
    }
    else {
      result$70 = (java.lang.String) (java.lang.String) in1.getField(8);
    }
    
    
    boolean isNull$57 = (java.lang.Integer) in1.getField(1) == null;
    int result$56;
    if (isNull$57) {
      result$56 = -1;
    }
    else {
      result$56 = (java.lang.Integer) in1.getField(1);
    }
    
    
    boolean isNull$69 = (java.lang.Integer) in1.getField(7) == null;
    int result$68;
    if (isNull$69) {
      result$68 = -1;
    }
    else {
      result$68 = (java.lang.Integer) in1.getField(7);
    }
    
    
    boolean isNull$61 = (java.lang.String) in1.getField(3) == null;
    java.lang.String result$60;
    if (isNull$61) {
      result$60 = "";
    }
    else {
      result$60 = (java.lang.String) (java.lang.String) in1.getField(3);
    }
    
    
    
    
    
    
    
    if (isNull$55) {
      out.setField(0, null);
    }
    else {
      out.setField(0, result$54);
    }
    
    
    
    if (isNull$57) {
      out.setField(1, null);
    }
    else {
      out.setField(1, result$56);
    }
    
    
    
    if (isNull$59) {
      out.setField(2, null);
    }
    else {
      out.setField(2, result$58);
    }
    
    
    
    if (isNull$61) {
      out.setField(3, null);
    }
    else {
      out.setField(3, result$60);
    }
    
    
    
    if (isNull$63) {
      out.setField(4, null);
    }
    else {
      out.setField(4, result$62);
    }
    
    
    
    if (isNull$65) {
      out.setField(5, null);
    }
    else {
      out.setField(5, result$64);
    }
    
    
    
    if (isNull$67) {
      out.setField(6, null);
    }
    else {
      out.setField(6, result$66);
    }
    
    
    
    if (isNull$69) {
      out.setField(7, null);
    }
    else {
      out.setField(7, result$68);
    }
    
    
    
    if (isNull$71) {
      out.setField(8, null);
    }
    else {
      out.setField(8, result$70);
    }
    
    
    
    
    
    boolean isNull$73 = isNull$63 || isNull$67;
    long result$72;
    if (isNull$73) {
      result$72 = -1L;
    }
    else {
      result$72 = (long) (result$62 * ((long) result$66));
    }
    
    if (isNull$73) {
      out.setField(9, null);
    }
    else {
      out.setField(9, result$72);
    }
    
    c.collect(out);
    
  }

  @Override
  public void close() throws Exception {
    
    
  }
}
