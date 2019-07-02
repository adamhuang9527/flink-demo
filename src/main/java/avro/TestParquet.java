package avro;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestParquet {
    private static final String csvDelimiter = ",";


    @Test
    public void test2() throws Exception {
        String inPath = "file:///Users/user/Documents/f84e39b3d561f42b-5529a34000000000_1424308034_data.1.parq";
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader.Builder<Group> reader = ParquetReader.builder(readSupport, new Path(inPath));
        ParquetReader<Group> build = reader.build();
        Group line = null;
        while ((line = build.read()) != null) {
            System.out.println(line);
        }
        System.out.println("读取结束");
    }

    @Test
    public void test1() throws IOException {
        AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(new Path("file:///tmp/flink-data/parquet/2019-06-26-1124/part-3-1"));
        GenericRecord record;
        while ((record = reader.read()) != null) {
            System.out.println(record);
        }


    }

    @Test
    public void test() throws IOException {
        viewParquet("file:///Users/user/Documents/f84e39b3d561f42b-5529a34000000000_1424308034_data.1.parq", 10);
    }


    public static Map<String, List<String[]>> viewParquet(String path, int maxLine) throws IOException {
        Map<String, List<String[]>> parquetInfo = new HashMap<>();
        List<String[]> dataList = new ArrayList<>();
        Schema.Field[] fields = null;
        String[] fieldNames = new String[0];

        ParquetReader<GenericData.Record> reader = AvroParquetReader.<GenericData.Record>builder(new Path(path)).build();
        int x = 0;
        GenericData.Record record;
        //解析Parquet数据逐行读取
        while ((record = reader.read()) != null && x < maxLine) {
            //读取第一行获取列头信息
            if (fields == null) {
                final List<Schema.Field> fieldsList = record.getSchema().getFields();
                fieldNames = getFieldNames(fields = fieldsList.toArray(new Schema.Field[0]));
                System.out.println("列头:" + String.join(csvDelimiter, fieldNames));
                dataList.add(fieldNames);
                parquetInfo.put("head", dataList);
                dataList = new ArrayList<>();
            }
            int i = 0;
            String[] dataString = new String[fieldNames.length];
            //读取数据获取列头信息
            for (final String fieldName : fieldNames) {
                String recordData = record.get(fieldName).toString();
                if (recordData.contains("type")) {
                    List<HashMap> dataFormValue = JSONArray.parseArray(JSONObject.parseObject(recordData).get("values").toString(), HashMap.class);
                    StringBuilder datas = new StringBuilder();
                    for (HashMap data : dataFormValue) {
                        datas.append(data.get("element").toString()).append(",");
                    }
                    datas.deleteCharAt(datas.length() - 1);
                    recordData = datas.toString();
                }
                dataString[i++] = recordData;
            }
            dataList.add(dataString);
            ++x;
        }

        parquetInfo.put("data", dataList);
        return parquetInfo;
    }

    private static String[] getFieldNames(final Schema.Field[] fields) {
        final String[] fieldNames = new String[fields.length];
        int i = 0;
        for (final Schema.Field field : fields) {
            fieldNames[i++] = field.name();
        }
        return fieldNames;
    }

}
