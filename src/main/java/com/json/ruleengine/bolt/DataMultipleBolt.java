package com.json.ruleengine.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Classname DataMultipleBolt
 * @Date 2018/11/9 下午2:10
 * @Create by yaolihua
 * @Description
 */
public class DataMultipleBolt implements IRichBolt {


    private OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {

        System.out.println("multiple:"+this.hashCode()+", 乘法:"+tuple.getStringByField("data")+", seq:"+tuple.getMessageId());
        JSONObject data = JSON.parseObject(tuple.getStringByField("data"));
        int a = data.getInteger("a");
        int b = data.getInteger("b");
        int id = data.getInteger("id");
        int multiple = a*b;
        List<Object> list = new ArrayList<Object>();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("table", "multiple_table_"+id);
        jsonObject.put("res", multiple);
        list.add(jsonObject);
        this.outputCollector.emit(list);

    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("multiple"));
    }
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("type", "multiple_bolt_"+this.hashCode());
        return map;
    }
}
