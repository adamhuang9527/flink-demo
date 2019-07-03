package genecode.table_function;

public class DataStreamCorrelateRule$34 extends org.apache.flink.streaming.api.functions.ProcessFunction {

  final org.apache.flink.table.runtime.TableFunctionCollector instance_org$apache$flink$table$runtime$TableFunctionCollector$29;
  
  final streaming.tablefunction.MySQLTableFunction function_streaming$tablefunction$MySQLTableFunction$34b85246193055906a897e03340f9c59;
  
  
  final org.apache.flink.types.Row out =
      new org.apache.flink.types.Row(9);
  
  

  public DataStreamCorrelateRule$34() throws Exception {
    
    function_streaming$tablefunction$MySQLTableFunction$34b85246193055906a897e03340f9c59 = (streaming.tablefunction.MySQLTableFunction)
    org.apache.flink.table.utils.EncodingUtils.decodeStringToObject(
      "rO0ABXNyACpzdHJlYW1pbmcudGFibGVmdW5jdGlvbi5NeVNRTFRhYmxlRnVuY3Rpb24wo95dmb7eGQIADUkACWNhY2hlU2l6ZUkACmNhY2hlVFRMTXNMAARjb25udAAVTGphdmEvc3FsL0Nvbm5lY3Rpb247WwAKZmlsZUZpZWxkc3QAE1tMamF2YS9sYW5nL1N0cmluZztMAAtmdW5uZWxDYWNoZXQAJkxjb20vZ29vZ2xlL2NvbW1vbi9jYWNoZS9Mb2FkaW5nQ2FjaGU7TAAIcGFzc3dvcmR0ABJMamF2YS9sYW5nL1N0cmluZztbAAtwcmltYXJ5S2V5c3EAfgACTAACcHN0ABxMamF2YS9zcWwvUHJlcGFyZWRTdGF0ZW1lbnQ7TAALcm93VHlwZUluZm90ADFMb3JnL2FwYWNoZS9mbGluay9hcGkvamF2YS90eXBldXRpbHMvUm93VHlwZUluZm87TAACcnN0ABRMamF2YS9zcWwvUmVzdWx0U2V0O0wACXRhYmxlTmFtZXEAfgAETAADdXJscQB-AARMAAh1c2VybmFtZXEAfgAEeHIALm9yZy5hcGFjaGUuZmxpbmsudGFibGUuZnVuY3Rpb25zLlRhYmxlRnVuY3Rpb27qA9IDIKYg-gIAAUwACWNvbGxlY3RvcnQAIUxvcmcvYXBhY2hlL2ZsaW5rL3V0aWwvQ29sbGVjdG9yO3hyADRvcmcuYXBhY2hlLmZsaW5rLnRhYmxlLmZ1bmN0aW9ucy5Vc2VyRGVmaW5lZEZ1bmN0aW9ug_bYS9VNpEsCAAB4cHAAAAPoADbugHBwcHQABHJvb3R1cgATW0xqYXZhLmxhbmcuU3RyaW5nO63SVufpHXtHAgAAeHAAAAABdAACaWRwc3IAL29yZy5hcGFjaGUuZmxpbmsuYXBpLmphdmEudHlwZXV0aWxzLlJvd1R5cGVJbmZvfxmYf1V00WsCAAJbABBjb21wYXJhdG9yT3JkZXJzdAACW1pbAApmaWVsZE5hbWVzcQB-AAJ4cgA1b3JnLmFwYWNoZS5mbGluay5hcGkuamF2YS50eXBldXRpbHMuVHVwbGVUeXBlSW5mb0Jhc2UAAAAAAAAAAQIAAkkAC3RvdGFsRmllbGRzWwAFdHlwZXN0ADdbTG9yZy9hcGFjaGUvZmxpbmsvYXBpL2NvbW1vbi90eXBlaW5mby9UeXBlSW5mb3JtYXRpb247eHIAM29yZy5hcGFjaGUuZmxpbmsuYXBpLmNvbW1vbi50eXBldXRpbHMuQ29tcG9zaXRlVHlwZQAAAAAAAAABAgABTAAJdHlwZUNsYXNzdAARTGphdmEvbGFuZy9DbGFzczt4cgA0b3JnLmFwYWNoZS5mbGluay5hcGkuY29tbW9uLnR5cGVpbmZvLlR5cGVJbmZvcm1hdGlvbpSNyEi6s3rrAgAAeHB2cgAab3JnLmFwYWNoZS5mbGluay50eXBlcy5Sb3cAAAAAAAAAAQIAAVsABmZpZWxkc3QAE1tMamF2YS9sYW5nL09iamVjdDt4cAAAAAR1cgA3W0xvcmcuYXBhY2hlLmZsaW5rLmFwaS5jb21tb24udHlwZWluZm8uVHlwZUluZm9ybWF0aW9uO7igbKQamhS2AgAAeHAAAAAEc3IAMm9yZy5hcGFjaGUuZmxpbmsuYXBpLmNvbW1vbi50eXBlaW5mby5CYXNpY1R5cGVJbmZv-gTwgqVp3QYCAARMAAVjbGF6enEAfgAVTAAPY29tcGFyYXRvckNsYXNzcQB-ABVbABdwb3NzaWJsZUNhc3RUYXJnZXRUeXBlc3QAEltMamF2YS9sYW5nL0NsYXNzO0wACnNlcmlhbGl6ZXJ0ADZMb3JnL2FwYWNoZS9mbGluay9hcGkvY29tbW9uL3R5cGV1dGlscy9UeXBlU2VyaWFsaXplcjt4cQB-ABZ2cgAQamF2YS5sYW5nLlN0cmluZ6DwpDh6O7NCAgAAeHB2cgA7b3JnLmFwYWNoZS5mbGluay5hcGkuY29tbW9uLnR5cGV1dGlscy5iYXNlLlN0cmluZ0NvbXBhcmF0b3IAAAAAAAAAAQIAAHhyAD5vcmcuYXBhY2hlLmZsaW5rLmFwaS5jb21tb24udHlwZXV0aWxzLmJhc2UuQmFzaWNUeXBlQ29tcGFyYXRvcgAAAAAAAAABAgACWgATYXNjZW5kaW5nQ29tcGFyaXNvblsAC2NvbXBhcmF0b3JzdAA3W0xvcmcvYXBhY2hlL2ZsaW5rL2FwaS9jb21tb24vdHlwZXV0aWxzL1R5cGVDb21wYXJhdG9yO3hyADRvcmcuYXBhY2hlLmZsaW5rLmFwaS5jb21tb24udHlwZXV0aWxzLlR5cGVDb21wYXJhdG9yAAAAAAAAAAECAAB4cHVyABJbTGphdmEubGFuZy5DbGFzczurFteuy81amQIAAHhwAAAAAHNyADtvcmcuYXBhY2hlLmZsaW5rLmFwaS5jb21tb24udHlwZXV0aWxzLmJhc2UuU3RyaW5nU2VyaWFsaXplcgAAAAAAAAABAgAAeHIAQm9yZy5hcGFjaGUuZmxpbmsuYXBpLmNvbW1vbi50eXBldXRpbHMuYmFzZS5UeXBlU2VyaWFsaXplclNpbmdsZXRvbnmph6rHLndFAgAAeHIANG9yZy5hcGFjaGUuZmxpbmsuYXBpLmNvbW1vbi50eXBldXRpbHMuVHlwZVNlcmlhbGl6ZXIAAAAAAAAAAQIAAHhwc3IANG9yZy5hcGFjaGUuZmxpbmsuYXBpLmNvbW1vbi50eXBlaW5mby5JbnRlZ2VyVHlwZUluZm-QBcRFaUBKlQIAAHhyADRvcmcuYXBhY2hlLmZsaW5rLmFwaS5jb21tb24udHlwZWluZm8uTnVtZXJpY1R5cGVJbmZvrZjGczACjBYCAAB4cQB-AB12cgARamF2YS5sYW5nLkludGVnZXIS4qCk94GHOAIAAUkABXZhbHVleHIAEGphdmEubGFuZy5OdW1iZXKGrJUdC5TgiwIAAHhwdnIAOG9yZy5hcGFjaGUuZmxpbmsuYXBpLmNvbW1vbi50eXBldXRpbHMuYmFzZS5JbnRDb21wYXJhdG9yAAAAAAAAAAECAAB4cQB-ACR1cQB-ACgAAAAEdnIADmphdmEubGFuZy5Mb25nO4vkkMyPI98CAAFKAAV2YWx1ZXhxAH4AMnZyAA9qYXZhLmxhbmcuRmxvYXTa7cmi2zzw7AIAAUYABXZhbHVleHEAfgAydnIAEGphdmEubGFuZy5Eb3VibGWAs8JKKWv7BAIAAUQABXZhbHVleHEAfgAydnIAE2phdmEubGFuZy5DaGFyYWN0ZXI0i0fZaxomeAIAAUMABXZhbHVleHBzcgA4b3JnLmFwYWNoZS5mbGluay5hcGkuY29tbW9uLnR5cGV1dGlscy5iYXNlLkludFNlcmlhbGl6ZXIAAAAAAAAAAQIAAHhxAH4AK3EAfgAwcQB-ACBwdXEAfgANAAAABHQACGN1Y2NlbmN5dAAEcmF0ZXEAfgAPdAAIcHJvdmluY2VwdAAIcHJvZHVjdDF0ABtqZGJjOm15c3FsOi8vbG9jYWxob3N0L3Rlc3RxAH4ADA",
      org.apache.flink.table.functions.UserDefinedFunction.class);
           
    
  }

  
  public DataStreamCorrelateRule$34(org.apache.flink.table.runtime.TableFunctionCollector arg0) throws Exception {
    this();
    instance_org$apache$flink$table$runtime$TableFunctionCollector$29 = arg0;
  
  }
  
  

  @Override
  public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
    
    function_streaming$tablefunction$MySQLTableFunction$34b85246193055906a897e03340f9c59.open(new org.apache.flink.table.functions.FunctionContext(getRuntimeContext()));
           
    
  }

  @Override
  public void processElement(Object _in1, org.apache.flink.streaming.api.functions.ProcessFunction.Context ctx, org.apache.flink.util.Collector c) throws Exception {
    org.apache.flink.types.Row in1 = (org.apache.flink.types.Row) _in1;
    
    boolean isNull$16 = (java.lang.Long) in1.getField(2) == null;
    long result$15;
    if (isNull$16) {
      result$15 = -1L;
    }
    else {
      result$15 = (java.lang.Long) in1.getField(2);
    }
    
    
    boolean isNull$12 = (java.lang.String) in1.getField(0) == null;
    java.lang.String result$11;
    if (isNull$12) {
      result$11 = "";
    }
    else {
      result$11 = (java.lang.String) (java.lang.String) in1.getField(0);
    }
    
    
    boolean isNull$20 = (java.lang.Long) in1.getField(4) == null;
    long result$19;
    if (isNull$20) {
      result$19 = -1L;
    }
    else {
      result$19 = (java.lang.Long) in1.getField(4);
    }
    
    
    boolean isNull$14 = (java.lang.Integer) in1.getField(1) == null;
    int result$13;
    if (isNull$14) {
      result$13 = -1;
    }
    else {
      result$13 = (java.lang.Integer) in1.getField(1);
    }
    
    
    boolean isNull$18 = (java.lang.String) in1.getField(3) == null;
    java.lang.String result$17;
    if (isNull$18) {
      result$17 = "";
    }
    else {
      result$17 = (java.lang.String) (java.lang.String) in1.getField(3);
    }
    
    
    
    
    
    function_streaming$tablefunction$MySQLTableFunction$34b85246193055906a897e03340f9c59.setCollector(instance_org$apache$flink$table$runtime$TableFunctionCollector$29);
    
    
    
    int result$32;
    boolean isNull$33;
    if (false) {
      result$32 = -1;
      isNull$33 = true;
    }
    else {
      
    boolean isNull$31 = (java.lang.Integer) in1.getField(1) == null;
    int result$30;
    if (isNull$31) {
      result$30 = -1;
    }
    else {
      result$30 = (java.lang.Integer) in1.getField(1);
    }
    
      result$32 = result$30;
      isNull$33 = isNull$31;
    }
    
    function_streaming$tablefunction$MySQLTableFunction$34b85246193055906a897e03340f9c59.eval(isNull$33 ? null : (java.lang.Integer) result$32);
    
    
    boolean hasOutput = instance_org$apache$flink$table$runtime$TableFunctionCollector$29.isCollected();
    if (!hasOutput) {
      
    
    if (isNull$12) {
      out.setField(0, null);
    }
    else {
      out.setField(0, result$11);
    }
    
    
    
    if (isNull$14) {
      out.setField(1, null);
    }
    else {
      out.setField(1, result$13);
    }
    
    
    
    if (isNull$16) {
      out.setField(2, null);
    }
    else {
      out.setField(2, result$15);
    }
    
    
    
    if (isNull$18) {
      out.setField(3, null);
    }
    else {
      out.setField(3, result$17);
    }
    
    
    
    if (isNull$20) {
      out.setField(4, null);
    }
    else {
      out.setField(4, result$19);
    }
    
    
    
    if (true) {
      out.setField(5, null);
    }
    else {
      out.setField(5, "");
    }
    
    
    
    if (true) {
      out.setField(6, null);
    }
    else {
      out.setField(6, -1);
    }
    
    
    
    if (true) {
      out.setField(7, null);
    }
    else {
      out.setField(7, -1);
    }
    
    
    
    if (true) {
      out.setField(8, null);
    }
    else {
      out.setField(8, "");
    }
    
      c.collect(out);
    }
    
  }

  @Override
  public void close() throws Exception {
    
    function_streaming$tablefunction$MySQLTableFunction$34b85246193055906a897e03340f9c59.close();
           
    
  }
}
