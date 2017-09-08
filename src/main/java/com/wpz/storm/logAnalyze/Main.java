package com.wpz.storm.logAnalyze;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.wpz.storm.wordCount.MySpout;

/**
 * @Author: wpz
 * @Desctription:
 * @Date: Created in 2017/9/8 17:09
 */
public class Main {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new ReaderSpout(), 4);
        builder.setBolt("area-bold", new GetAreaBolt()).shuffleGrouping("spout");
        builder.setBolt("logitude-bolt", new GetLongitudeBolt()).shuffleGrouping("area-bolt");

        Config config = new Config();
        config.setNumWorkers(4);
        config.setMaxSpoutPending(1000);
        StormSubmitter.submitTopology("area-topology", config, builder.createTopology());

//        LocalCluster localCluster = new LocalCluster();
//        localCluster.submitTopology("mywordcount",config,builder.createTopology());
    }
}
