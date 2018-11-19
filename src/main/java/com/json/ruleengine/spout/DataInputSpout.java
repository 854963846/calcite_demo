package com.json.ruleengine.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Classname DataInputSpout
 * @Date 2018/11/9 上午9:49
 * @Create by yaolihua
 * @Description
 */
public class DataInputSpout implements IRichSpout{

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("data"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private SpoutOutputCollector outputCollector;
    private AtomicInteger seq;
    private List<String> wordList;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector = spoutOutputCollector;
        seq = new AtomicInteger();

        wordList = new ArrayList<String>();
        wordList.add("{\"a\":\"2\", \"b\":\"1\"}");
        wordList.add("{\"a\":\"4\", \"b\":\"2\"}");
        wordList.add("{\"a\":\"3\", \"b\":\"3\"}");
        wordList.add("{\"a\":\"1\", \"b\":\"4\"}");
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }


    public void nextTuple() {
        List<Object> data = new ArrayList<Object>();
        int sequence = seq.getAndIncrement();
        String json = wordList.get(sequence%4);
        JSONObject jsonObject = JSON.parseObject(json);
        jsonObject.put("id", sequence);
        data.add(jsonObject.toJSONString());
        System.out.println("spout send:"+jsonObject.toJSONString());
        this.outputCollector.emit(data, sequence);
    }

    public void ack(Object o) {

    }

    public void fail(Object o) {

    }
}
