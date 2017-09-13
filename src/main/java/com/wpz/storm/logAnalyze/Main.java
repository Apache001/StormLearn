package com.wpz.storm.logAnalyze;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.wpz.storm.logAnalyze.GetAreaBolt;
import com.wpz.storm.logAnalyze.GetLongitudeBolt;
import com.wpz.storm.logAnalyze.ReaderSpout;

public class Main {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new ReaderSpout(), 4);
        //4 the number of tasks that should be assigned to execute this spout
        builder.setBolt("area-bolt", new GetAreaBolt()).shuffleGrouping("spout");
        builder.setBolt("longitude-bolt", new GetLongitudeBolt()).shuffleGrouping("area-bolt");

        Config config = new Config();
        config.setNumWorkers(4);
        config.setMaxSpoutPending(1000);
//        StormSubmitter.submitTopology("area-topology", config,
//                builder.createTopology());

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("area-topology",config,builder.createTopology());

    }
}
