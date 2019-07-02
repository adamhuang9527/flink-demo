package submit;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParseJsonPlan {



    //key 是sink id，value为该key对应的sourceids
    static Map<Integer, List<Integer>> sinkSources = new HashMap<>();

    public static void main(String[] args) throws IOException {
        String json = FileUtils.readFileToString(new File("/tmp/aaa.txt"), "UTF-8");
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONArray jsonArray = jsonObject.getJSONArray("nodes");

        Map<Integer, JSONObject> nodes = new HashMap<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject node = jsonArray.getJSONObject(i);
            nodes.put(node.getInteger("id"), node);


            if ("Data Sink".equals(node.getString("pact"))) {
                int sinkid = node.getInteger("id");
                sinkSources.put(sinkid, new ArrayList<>());
                getParents(sinkid, sinkid, nodes);
            }
        }

        System.out.println(sinkSources);
//        System.out.println(jsonArray);
//        System.out.println(jsonObject);
    }

    private static void getParents(int sinkId, int currentId, Map<Integer, JSONObject> nodes) {
        //递归结束
        JSONObject node = nodes.get(currentId);
        if (!node.containsKey("predecessors") && node.getString("pact").equals("Data Source")) {
            sinkSources.get(sinkId).add(node.getInteger("id"));
            return;
        } else {
            JSONArray predecessors = node.getJSONArray("predecessors");
            for (int i = 0; i < predecessors.size(); i++) {
                JSONObject predecessor = predecessors.getJSONObject(i);
                int nid = predecessor.getInteger("id");
                getParents(sinkId, nid, nodes);
            }
        }

    }


}
