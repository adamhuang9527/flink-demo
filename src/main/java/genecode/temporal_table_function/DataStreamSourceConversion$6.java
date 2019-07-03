package genecode.temporal_table_function;

public class DataStreamSourceConversion$6 extends org.apache.flink.streaming.api.functions.ProcessFunction {

  
  final org.apache.flink.types.Row out =
      new org.apache.flink.types.Row(3);
  
  

  public DataStreamSourceConversion$6() throws Exception {
    
    
  }

  
  

  @Override
  public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
    
    
  }

  @Override
  public void processElement(Object _in1, org.apache.flink.streaming.api.functions.ProcessFunction.Context ctx, org.apache.flink.util.Collector c) throws Exception {
    org.apache.flink.api.java.tuple.Tuple2 in1 = (org.apache.flink.api.java.tuple.Tuple2) _in1;
    
    boolean isNull$1 = (java.lang.Long) in1.f0 == null;
    long result$0;
    if (isNull$1) {
      result$0 = -1L;
    }
    else {
      result$0 = (java.lang.Long) in1.f0;
    }
    
    
    boolean isNull$3 = (java.lang.String) in1.f1 == null;
    java.lang.String result$2;
    if (isNull$3) {
      result$2 = "";
    }
    else {
      result$2 = (java.lang.String) (java.lang.String) in1.f1;
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
    
    
    
    
    long result$4 = -1L;
    
    java.lang.Long result$5;
    if (true) {
      result$5 = null;
    }
    else {
      result$5 = result$4;
    }
    
    if (true) {
      out.setField(2, null);
    }
    else {
      out.setField(2, result$5);
    }
    
    c.collect(out);
    
  }

  @Override
  public void close() throws Exception {
    
    
  }
}