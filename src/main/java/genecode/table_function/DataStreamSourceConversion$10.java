package genecode.table_function;

public class DataStreamSourceConversion$10 extends org.apache.flink.streaming.api.functions.ProcessFunction {

  
  final org.apache.flink.types.Row out =
      new org.apache.flink.types.Row(5);
  
  

  public DataStreamSourceConversion$10() throws Exception {
    
    
  }

  
  

  @Override
  public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
    
    
  }

  @Override
  public void processElement(Object _in1, org.apache.flink.streaming.api.functions.ProcessFunction.Context ctx, org.apache.flink.util.Collector c) throws Exception {
    org.apache.flink.api.java.tuple.Tuple5 in1 = (org.apache.flink.api.java.tuple.Tuple5) _in1;
    
    boolean isNull$5 = (java.lang.Long) in1.f2 == null;
    long result$4;
    if (isNull$5) {
      result$4 = -1L;
    }
    else {
      result$4 = (java.lang.Long) in1.f2;
    }
    
    
    boolean isNull$1 = (java.lang.String) in1.f0 == null;
    java.lang.String result$0;
    if (isNull$1) {
      result$0 = "";
    }
    else {
      result$0 = (java.lang.String) (java.lang.String) in1.f0;
    }
    
    
    boolean isNull$9 = (java.lang.Long) in1.f4 == null;
    long result$8;
    if (isNull$9) {
      result$8 = -1L;
    }
    else {
      result$8 = (java.lang.Long) in1.f4;
    }
    
    
    boolean isNull$3 = (java.lang.Integer) in1.f1 == null;
    int result$2;
    if (isNull$3) {
      result$2 = -1;
    }
    else {
      result$2 = (java.lang.Integer) in1.f1;
    }
    
    
    boolean isNull$7 = (java.lang.String) in1.f3 == null;
    java.lang.String result$6;
    if (isNull$7) {
      result$6 = "";
    }
    else {
      result$6 = (java.lang.String) (java.lang.String) in1.f3;
    }
    
    
    
    
    
    
    
    if (isNull$1) {
      out.setField(0, null);
    }
    else {
      out.setField(0, result$0);
    }
    
    
    
    if (isNull$3) {
      out.setField(1, null);
    }
    else {
      out.setField(1, result$2);
    }
    
    
    
    if (isNull$5) {
      out.setField(2, null);
    }
    else {
      out.setField(2, result$4);
    }
    
    
    
    if (isNull$7) {
      out.setField(3, null);
    }
    else {
      out.setField(3, result$6);
    }
    
    
    
    if (isNull$9) {
      out.setField(4, null);
    }
    else {
      out.setField(4, result$8);
    }
    
    c.collect(out);
    
  }

  @Override
  public void close() throws Exception {
    
    
  }
}
