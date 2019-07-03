package genecode.temporal_table_function;

public class DataStreamCalcRule$40 extends org.apache.flink.streaming.api.functions.ProcessFunction {

  
  final org.apache.flink.types.Row out =
      new org.apache.flink.types.Row(4);
  
  

  public DataStreamCalcRule$40() throws Exception {
    
    
  }

  
  

  @Override
  public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
    
    
  }

  @Override
  public void processElement(Object _in1, org.apache.flink.streaming.api.functions.ProcessFunction.Context ctx, org.apache.flink.util.Collector c) throws Exception {
    org.apache.flink.types.Row in1 = (org.apache.flink.types.Row) _in1;
    
    boolean isNull$35 = (java.lang.Long) in1.getField(0) == null;
    long result$34;
    if (isNull$35) {
      result$34 = -1L;
    }
    else {
      result$34 = (java.lang.Long) in1.getField(0);
    }
    
    
    boolean isNull$37 = (java.lang.Long) in1.getField(4) == null;
    long result$36;
    if (isNull$37) {
      result$36 = -1L;
    }
    else {
      result$36 = (java.lang.Long) in1.getField(4);
    }
    
    
    boolean isNull$33 = (java.lang.String) in1.getField(1) == null;
    java.lang.String result$32;
    if (isNull$33) {
      result$32 = "";
    }
    else {
      result$32 = (java.lang.String) (java.lang.String) in1.getField(1);
    }
    
    
    
    
    
    
    
    if (isNull$33) {
      out.setField(0, null);
    }
    else {
      out.setField(0, result$32);
    }
    
    
    
    if (isNull$35) {
      out.setField(1, null);
    }
    else {
      out.setField(1, result$34);
    }
    
    
    
    if (isNull$37) {
      out.setField(2, null);
    }
    else {
      out.setField(2, result$36);
    }
    
    
    
    
    
    boolean isNull$39 = isNull$35 || isNull$37;
    long result$38;
    if (isNull$39) {
      result$38 = -1L;
    }
    else {
      result$38 = (long) (result$34 * result$36);
    }
    
    if (isNull$39) {
      out.setField(3, null);
    }
    else {
      out.setField(3, result$38);
    }
    
    c.collect(out);
    
  }

  @Override
  public void close() throws Exception {
    
    
  }
}
