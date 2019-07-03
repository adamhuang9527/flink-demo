package genecode.temporal_table_function;

public class DataStreamCalcRule$18 extends org.apache.flink.streaming.api.functions.ProcessFunction {

  
  final org.apache.flink.types.Row out =
      new org.apache.flink.types.Row(2);
  
  

  public DataStreamCalcRule$18() throws Exception {
    
    
  }

  
  

  @Override
  public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
    
    
  }

  @Override
  public void processElement(Object _in1, org.apache.flink.streaming.api.functions.ProcessFunction.Context ctx, org.apache.flink.util.Collector c) throws Exception {
    org.apache.flink.types.Row in1 = (org.apache.flink.types.Row) _in1;
    
    boolean isNull$15 = (java.lang.String) in1.getField(0) == null;
    java.lang.String result$14;
    if (isNull$15) {
      result$14 = "";
    }
    else {
      result$14 = (java.lang.String) (java.lang.String) in1.getField(0);
    }
    
    
    boolean isNull$17 = (java.lang.Long) in1.getField(1) == null;
    long result$16;
    if (isNull$17) {
      result$16 = -1L;
    }
    else {
      result$16 = (java.lang.Long) in1.getField(1);
    }
    
    
    
    
    
    
    
    if (isNull$15) {
      out.setField(0, null);
    }
    else {
      out.setField(0, result$14);
    }
    
    
    
    if (isNull$17) {
      out.setField(1, null);
    }
    else {
      out.setField(1, result$16);
    }
    
    c.collect(out);
    
  }

  @Override
  public void close() throws Exception {
    
    
  }
}