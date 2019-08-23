package genecode.table_function;

public class TableFunctionCollector$53 extends org.apache.flink.table.runtime.TableFunctionCollector {

  
  
  final org.apache.flink.types.Row out =
      new org.apache.flink.types.Row(9);
  
  

  public TableFunctionCollector$53() throws Exception {
    
    
  }

  @Override
  public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
    
    
  }

  @Override
  public void collect(Object record) {
    super.collect(record);
    org.apache.flink.types.Row in1 = (org.apache.flink.types.Row) getInput();
    org.apache.flink.types.Row in2 = (org.apache.flink.types.Row) record;
    
    boolean isNull$40 = (java.lang.Long) in1.getField(2) == null;
    long result$39;
    if (isNull$40) {
      result$39 = -1L;
    }
    else {
      result$39 = (java.lang.Long) in1.getField(2);
    }
    
    
    boolean isNull$36 = (java.lang.String) in1.getField(0) == null;
    java.lang.String result$35;
    if (isNull$36) {
      result$35 = "";
    }
    else {
      result$35 = (java.lang.String) (java.lang.String) in1.getField(0);
    }
    
    
    boolean isNull$44 = (java.lang.Long) in1.getField(4) == null;
    long result$43;
    if (isNull$44) {
      result$43 = -1L;
    }
    else {
      result$43 = (java.lang.Long) in1.getField(4);
    }
    
    
    boolean isNull$38 = (java.lang.Integer) in1.getField(1) == null;
    int result$37;
    if (isNull$38) {
      result$37 = -1;
    }
    else {
      result$37 = (java.lang.Integer) in1.getField(1);
    }
    
    
    boolean isNull$42 = (java.lang.String) in1.getField(3) == null;
    java.lang.String result$41;
    if (isNull$42) {
      result$41 = "";
    }
    else {
      result$41 = (java.lang.String) (java.lang.String) in1.getField(3);
    }
    
    
    
    
    
    
    
    if (isNull$36) {
      out.setField(0, null);
    }
    else {
      out.setField(0, result$35);
    }
    
    
    
    if (isNull$38) {
      out.setField(1, null);
    }
    else {
      out.setField(1, result$37);
    }
    
    
    
    if (isNull$40) {
      out.setField(2, null);
    }
    else {
      out.setField(2, result$39);
    }
    
    
    
    if (isNull$42) {
      out.setField(3, null);
    }
    else {
      out.setField(3, result$41);
    }
    
    
    
    if (isNull$44) {
      out.setField(4, null);
    }
    else {
      out.setField(4, result$43);
    }
    
    
    
    boolean isNull$46 = (java.lang.String) in2.getField(0) == null;
    java.lang.String result$45;
    if (isNull$46) {
      result$45 = "";
    }
    else {
      result$45 = (java.lang.String) (java.lang.String) in2.getField(0);
    }
    
    if (isNull$46) {
      out.setField(5, null);
    }
    else {
      out.setField(5, result$45);
    }
    
    
    
    boolean isNull$48 = (java.lang.Integer) in2.getField(1) == null;
    int result$47;
    if (isNull$48) {
      result$47 = -1;
    }
    else {
      result$47 = (java.lang.Integer) in2.getField(1);
    }
    
    if (isNull$48) {
      out.setField(6, null);
    }
    else {
      out.setField(6, result$47);
    }
    
    
    
    boolean isNull$50 = (java.lang.Integer) in2.getField(2) == null;
    int result$49;
    if (isNull$50) {
      result$49 = -1;
    }
    else {
      result$49 = (java.lang.Integer) in2.getField(2);
    }
    
    if (isNull$50) {
      out.setField(7, null);
    }
    else {
      out.setField(7, result$49);
    }
    
    
    
    boolean isNull$52 = (java.lang.String) in2.getField(3) == null;
    java.lang.String result$51;
    if (isNull$52) {
      result$51 = "";
    }
    else {
      result$51 = (java.lang.String) (java.lang.String) in2.getField(3);
    }
    
    if (isNull$52) {
      out.setField(8, null);
    }
    else {
      out.setField(8, result$51);
    }
    
    getCollector().collect(out);
    
  }

  @Override
  public void close() {
    
    
  }
}
