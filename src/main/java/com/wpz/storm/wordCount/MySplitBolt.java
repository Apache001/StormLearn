package com.wpz.storm.wordCount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;


public class MySplitBolt extends BaseRichBolt {
    OutputCollector collector;
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }


    public void execute(Tuple input) {
        String line=input.getString(0);
        String[] arrWords = line.split(" ");
        for (String word:arrWords){
            collector.emit(new Values(word,1));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","num"));
    }
}
