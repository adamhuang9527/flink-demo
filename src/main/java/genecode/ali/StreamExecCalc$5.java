package genecode.ali;

public class StreamExecCalc$5 extends org.apache.flink.table.runtime.AbstractProcessStreamOperator
          implements org.apache.flink.streaming.api.operators.OneInputStreamOperator {

        private final Object[] references;
        final org.apache.flink.table.dataformat.BoxedWrapperRow out = new org.apache.flink.table.dataformat.BoxedWrapperRow(3);
        private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement = new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);

        public StreamExecCalc$5(
            Object[] references,
            org.apache.flink.streaming.runtime.tasks.StreamTask task,
            org.apache.flink.streaming.api.graph.StreamConfig config,
            org.apache.flink.streaming.api.operators.Output output) throws Exception {
          this.references = references;
          
          this.setup(task, config, output);
        }

        @Override
        public void open() throws Exception {
          super.open();
          
        }

        @Override
        public void processElement(org.apache.flink.streaming.runtime.streamrecord.StreamRecord element) throws Exception {
          org.apache.flink.table.dataformat.BaseRow in1 = (org.apache.flink.table.dataformat.BaseRow) element.getValue();
          
          
          
          
          
          
          out.setHeader(in1.getHeader());
          
          
          
          
          if (in1.isNullAt(2)) {
            out.setNullAt(2);
          } else {
            out.setNonPrimitiveValue(2, in1.getString(2));
          }
                    
          
          
          if (in1.isNullAt(0)) {
            out.setNullAt(0);
          } else {
            out.setLong(0, in1.getLong(0));
          }
                    
          
          
          if (in1.isNullAt(1)) {
            out.setNullAt(1);
          } else {
            out.setInt(1, in1.getInt(1));
          }
                    
                  
          output.collect(outElement.replace(out));
          
          
        }

        // TODO @Override
        public void endInput() throws Exception {
          
          
        }

        @Override
        public void close() throws Exception {
           // TODO remove it after introduce endInput in runtime.
           endInput();
           super.close();
          
        }

        
      }
    