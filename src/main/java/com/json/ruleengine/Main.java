package com.json.ruleengine;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import com.json.ruleengine.bolt.DataCountBolt;
import com.json.ruleengine.bolt.DataMultipleBolt;
import com.json.ruleengine.bolt.DataSumBolt;
import com.json.ruleengine.spout.DataInputSpout;

import java.util.HashMap;
import java.util.Map;

/**
 * @Classname Main
 * @Date 2018/11/9 上午9:40
 * @Create by yaolihua
 * @Description
 */
public class Main {

    public static void main(String[] args)
    {
        TopologyBuilder builder =  new TopologyBuilder();
        builder.setSpout("data_input_spout", new DataInputSpout());
        builder.setBolt("data_sum_bolt", new DataSumBolt(), 1).noneGrouping("data_input_spout");
        builder.setBolt("data_multiple_bolt", new DataMultipleBolt(), 1).noneGrouping("data_input_spout");

        BoltDeclarer countBolt = builder.setBolt("data_count_bolt", new DataCountBolt(), 1);
        countBolt.noneGrouping("data_sum_bolt");
        countBolt.noneGrouping("data_multiple_bolt");


        Map conf = new HashMap();
        conf.put(Config.TOPOLOGY_WORKERS, 1);
//        LocalCluster localCluster = new LocalCluster();
//        localCluster.submitTopology("word_count_topology", conf, builder.createTopology());
        try {
            StormSubmitter.submitTopology("word_count_topology", conf, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }
    }

}
