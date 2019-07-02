package table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.TableSource;

public class NestingJsonKafkaSql {
    public static void main(String[] args) {
    }


    public static class MyTableSource implements TableSource{

        @Override
        public TypeInformation getReturnType() {
            return null;
        }

        @Override
        public TableSchema getTableSchema() {
            return null;
        }


    }
}
