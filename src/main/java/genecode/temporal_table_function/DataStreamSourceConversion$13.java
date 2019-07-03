package genecode.temporal_table_function;

public class DataStreamSourceConversion$13 extends org.apache.flink.streaming.api.functions.ProcessFunction {

  
  final org.apache.flink.types.Row out =
      new org.apache.flink.types.Row(3);
  
  

  public DataStreamSourceConversion$13() throws Exception {
    
    
  }

  
  

  @Override
  public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
    
    
  }

  @Override
  public void processElement(Object _in1, org.apache.flink.streaming.api.functions.ProcessFunction.Context ctx, org.apache.flink.util.Collector c) throws Exception {
    org.apache.flink.api.java.tuple.Tuple2 in1 = (org.apache.flink.api.java.tuple.Tuple2) _in1;
    
    boolean isNull$8 = (java.lang.String) in1.f0 == null;
    java.lang.String result$7;
    if (isNull$8) {
      result$7 = "";
    }
    else {
      result$7 = (java.lang.String) (java.lang.String) in1.f0;
    }
    
    
    boolean isNull$10 = (java.lang.Long) in1.f1 == null;
    long result$9;
    if (isNull$10) {
      result$9 = -1L;
    }
    else {
      result$9 = (java.lang.Long) in1.f1;
    }
    
    
    
    
    
    
    
    if (isNull$8) {
      out.setField(0, null);
    }
    else {
      out.setField(0, result$7);
    }
    
    
    
    if (isNull$10) {
      out.setField(1, null);
    }
    else {
      out.setField(1, result$9);
    }
    
    
    
    
    long result$11 = -1L;
    
    java.lang.Long result$12;
    if (true) {
      result$12 = null;
    }
    else {
      result$12 = result$11;
    }
    
    if (true) {
      out.setField(2, null);
    }
    else {
      out.setField(2, result$12);
    }
    
    c.collect(out);
    
  }

  @Override
  public void close() throws Exception {
    
    
  }
}