package com.json.ruleengine.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.json.ruleengine.calcite.SchemaManage;
import com.json.ruleengine.calcite.table.CalculateTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.lang.ObjectUtils;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Classname DataCountBolt
 * @Date 2018/11/9 下午2:17
 * @Create by yaolihua
 * @Description
 */
public class DataCountBolt implements IRichBolt {
    private OutputCollector outputCollector;
    Map<String, JSONObject> tableMap;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        tableMap = new HashMap<String, JSONObject>();
    }

    public void execute(Tuple tuple) {
//        final SchemaPlus rootSchema = SchemaManage.INSTANCE.getRootSchema();
//        JSONObject data = JSON.parseObject(tuple.getStringByField("data"));
//        String table = data.getString("table");
//
//        JSONObject multipleTable = null;
//        JSONObject sumTable = null;
//        if(table.contains("sum")) {
//            sumTable = data;
//            multipleTable = tableMap.get(table.replace("sum", "multiple"));
//        } else {
//            multipleTable = data;
//            sumTable = tableMap.get(table.replace("sum", "multiple"));
//        }
//
//        if(null != sumTable && null != multipleTable)
//        {
//            CalculateTable sum = new CalculateTable(sumTable);
//            String t1 = sum.getTableName();
//            rootSchema.add(t1, sum);
//
//            CalculateTable multiple = new CalculateTable(multipleTable);
//            String t2 = sum.getTableName();
//            rootSchema.add(t2, multiple);
//
//            ResultSet rs = SchemaManage.INSTANCE.executeSQL("select");
//            System.out.println(rs);
//        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
