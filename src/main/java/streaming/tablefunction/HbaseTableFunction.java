package streaming.tablefunction;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class HbaseTableFunction extends TableFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(HbaseTableFunction.class);

    //必填字段
    private RowTypeInfo rowTypeInfo;
    private String zkQuorum;
    private String tableName;
    private String rowKey;


    //选填字段
    private int cacheSize;
    private int cacheTTLMs;


    private TypeInformation rowKeyType;
    private transient Table table;
    private transient Connection connection = null;
    private static final int cacheSizeDefaultValue = 1000;
    private static final int cacheTTLMsDefaultValue = 1000 * 60 * 60;
    private static final String FAMILY_QUAFILIER_SPLIT = "\\.";
    private int rowKeyTypeIndex;
    private List<Tuple4<byte[], byte[], TypeInformation<?>, Integer>> qualifierList;
    //在传入的字段数组中，rowkey的位置
    private int rowKeyIndex;
    public LoadingCache<Object, Row> funnelCache = null;

    public HbaseTableFunction(RowTypeInfo rowTypeInfo, String zkQuorum, String tableName, String rowKey) {
        this(rowTypeInfo, zkQuorum, tableName, rowKey, cacheSizeDefaultValue, cacheTTLMsDefaultValue);
    }

    public HbaseTableFunction(RowTypeInfo rowTypeInfo, String zkQuorum, String tableName, String rowKey, int cacheSize, int cacheTTLMs) {
        this.rowTypeInfo = rowTypeInfo;
        this.zkQuorum = zkQuorum;
        this.tableName = tableName;
        this.rowKey = rowKey;
        this.cacheSize = cacheSize;
        this.cacheTTLMs = cacheTTLMs;
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(rowTypeInfo.getFieldNames(), rowTypeInfo.getFieldTypes());
    }


    public void eval(Object key) throws ExecutionException {
       Row row = funnelCache.get(key);
       if(row != null){
           collect(row);
       }
    }

    private byte[] createRowKey(Object key) throws UnsupportedEncodingException {
        rowKeyTypeIndex = HBaseTypeUtils.getTypeIndex(rowKeyType.getTypeClass());
        byte[] res = HBaseTypeUtils.serializeFromInternalObject(key, rowKeyTypeIndex, "UTF-8");
        return res;
    }

    @Override
    public void open(FunctionContext context) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkQuorum);
        try {
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            LOG.error("init table exception ", e);
            throw e;
        }


        String[] fieldNames = rowTypeInfo.getFieldNames();
        TypeInformation[] types = rowTypeInfo.getFieldTypes();
        int length = fieldNames.length;

        qualifierList = new ArrayList<>();

        for (int i = 0; i < length; i++) {
            if (fieldNames[i].equals(rowKey)) {
                rowKeyType = types[i];
                rowKeyIndex = i;
            } else {
                String[] ss = fieldNames[i].split(FAMILY_QUAFILIER_SPLIT);
                Tuple4<byte[], byte[], TypeInformation<?>, Integer> q = new Tuple4<>(Bytes.toBytes(ss[0]), Bytes.toBytes(ss[1]), types[i], i);
                qualifierList.add(q);
            }
        }


        funnelCache = CacheBuilder.newBuilder().maximumSize(cacheSize)
                .expireAfterAccess(cacheTTLMs, TimeUnit.MILLISECONDS).build(new CacheLoader<Object, Row>() {
                    @Override
                    public Row load(Object rowKey) throws Exception {
                        return getRowData(rowKey);
                    }
                });
    }

    private Row getRowData(Object rowKey) throws IOException {

        Get get = new Get(createRowKey(rowKey));
        Result result = table.get(get);
        Row row = new Row(rowTypeInfo.getArity());
        if (!result.isEmpty()) {
            row.setField(rowKeyIndex, rowKey);

            for (Tuple4<byte[], byte[], TypeInformation<?>, Integer> qualifier : qualifierList) {
                byte[] value = result.getValue(qualifier.f0, qualifier.f1);
                if(value != null){
                    Object obj = HBaseTypeUtils.deserializeToObject(value, HBaseTypeUtils.getTypeIndex(qualifier.f2.getTypeClass()), "UTF-8");
                    row.setField(qualifier.f3, obj);
                }else{
                    row.setField(qualifier.f3, null);
                }

            }

        }
        return row;
    }


    @Override
    public void close() throws Exception {
        LOG.info("start close ...");
        if (null != table) {
            try {
                table.close();
            } catch (IOException e) {
                LOG.warn("exception when close table", e);
            }
        }
        if (null != connection) {
            try {
                connection.close();
            } catch (IOException e) {
                LOG.warn("exception when close connection", e);
            }
        }
        LOG.info("end close.");
    }
}
