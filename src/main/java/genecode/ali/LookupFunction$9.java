package genecode.ali;

public class LookupFunction$9
          extends org.apache.flink.api.common.functions.RichFlatMapFunction {

        private transient org.apache.flink.table.runtime.utils.InMemoryLookupableTableSource.InMemoryLookupFunction function_org$apache$flink$table$runtime$utils$InMemoryLookupableTableSource$InMemoryLookupFunction$22544e4eacc4eb965ad73334b49831af;
        private transient org.apache.flink.table.codegen.LookupJoinCodeGenerator.RowToBaseRowCollector collector$8;

        public LookupFunction$9(Object[] references) throws Exception {
          function_org$apache$flink$table$runtime$utils$InMemoryLookupableTableSource$InMemoryLookupFunction$22544e4eacc4eb965ad73334b49831af = (((org.apache.flink.table.runtime.utils.InMemoryLookupableTableSource.InMemoryLookupFunction) references[0]));
          collector$8 = (((org.apache.flink.table.codegen.LookupJoinCodeGenerator.RowToBaseRowCollector) references[1]));
        }

        

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
          
          function_org$apache$flink$table$runtime$utils$InMemoryLookupableTableSource$InMemoryLookupFunction$22544e4eacc4eb965ad73334b49831af.open(new org.apache.flink.table.functions.FunctionContext(getRuntimeContext()));
                 
        }

        @Override
        public void flatMap(Object _in1, org.apache.flink.util.Collector c) throws Exception {
          org.apache.flink.table.dataformat.BaseRow in1 = (org.apache.flink.table.dataformat.BaseRow) _in1;
          
          long field$6;
          boolean isNull$6;
          isNull$6 = in1.isNullAt(0);
          field$6 = -1L;
          if (!isNull$6) {
            field$6 = in1.getLong(0);
          }
          
          
          java.lang.Long arg$7 = null;
          if (!isNull$6) {
            arg$7 = (java.lang.Long) field$6;
          }
                       
          
          collector$8.setCollector(c);
          function_org$apache$flink$table$runtime$utils$InMemoryLookupableTableSource$InMemoryLookupFunction$22544e4eacc4eb965ad73334b49831af.setCollector(collector$8);
                   
          function_org$apache$flink$table$runtime$utils$InMemoryLookupableTableSource$InMemoryLookupFunction$22544e4eacc4eb965ad73334b49831af.eval(arg$7);
                
        }

        @Override
        public void close() throws Exception {
          
          function_org$apache$flink$table$runtime$utils$InMemoryLookupableTableSource$InMemoryLookupFunction$22544e4eacc4eb965ad73334b49831af.close();
                 
        }
      }
    