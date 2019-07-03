package genecode.temporal_table_function;

public class DataStreamTemporalTableJoinRule$31 extends org.apache.flink.api.common.functions.RichFlatJoinFunction {

  
  final org.apache.flink.types.Row out =
      new org.apache.flink.types.Row(5);
  
  

  public DataStreamTemporalTableJoinRule$31() throws Exception {
    
    
  }

  
  

  @Override
  public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
    
    
  }

  @Override
  public void join(Object _in1, Object _in2, org.apache.flink.util.Collector c) throws Exception {
    org.apache.flink.types.Row in1 = (org.apache.flink.types.Row) _in1;
    org.apache.flink.types.Row in2 = (org.apache.flink.types.Row) _in2;
    
    boolean isNull$24 = (java.lang.Long) in1.getField(2) == null;
    long result$23;
    if (isNull$24) {
      result$23 = -1L;
    }
    else {
      result$23 = (long) org.apache.calcite.runtime.SqlFunctions.toLong((java.lang.Long) in1.getField(2));
    }
    
    
    boolean isNull$20 = (java.lang.Long) in1.getField(0) == null;
    long result$19;
    if (isNull$20) {
      result$19 = -1L;
    }
    else {
      result$19 = (java.lang.Long) in1.getField(0);
    }
    
    
    boolean isNull$28 = (java.lang.Long) in2.getField(1) == null;
    long result$27;
    if (isNull$28) {
      result$27 = -1L;
    }
    else {
      result$27 = (java.lang.Long) in2.getField(1);
    }
    
    
    boolean isNull$26 = (java.lang.String) in2.getField(0) == null;
    java.lang.String result$25;
    if (isNull$26) {
      result$25 = "";
    }
    else {
      result$25 = (java.lang.String) (java.lang.String) in2.getField(0);
    }
    
    
    boolean isNull$22 = (java.lang.String) in1.getField(1) == null;
    java.lang.String result$21;
    if (isNull$22) {
      result$21 = "";
    }
    else {
      result$21 = (java.lang.String) (java.lang.String) in1.getField(1);
    }
    
    
    
    
    
    
    boolean result$30 = true;
    
    if (result$30) {
      
    
    if (isNull$20) {
      out.setField(0, null);
    }
    else {
      out.setField(0, result$19);
    }
    
    
    
    if (isNull$22) {
      out.setField(1, null);
    }
    else {
      out.setField(1, result$21);
    }
    
    
    
    
    java.lang.Long result$29;
    if (isNull$24) {
      result$29 = null;
    }
    else {
      result$29 = result$23;
    }
    
    if (isNull$24) {
      out.setField(2, null);
    }
    else {
      out.setField(2, result$29);
    }
    
    
    
    if (isNull$26) {
      out.setField(3, null);
    }
    else {
      out.setField(3, result$25);
    }
    
    
    
    if (isNull$28) {
      out.setField(4, null);
    }
    else {
      out.setField(4, result$27);
    }
    
      c.collect(out);
    }
    
  }

  @Override
  public void close() throws Exception {
    
    
  }
}
