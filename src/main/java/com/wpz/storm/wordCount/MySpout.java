package com.wpz.storm.wordCount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.Random;



public class MySpout extends BaseRichSpout {
    SpoutOutputCollector collector;
    private FileReader fileReader;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
//        try{
//            this.fileReader=new FileReader(conf.get("wordsFile").toString());
//        }catch (FileNotFoundException e){
//            throw new RuntimeException("Error reading file ["
//                    + conf.get("wordFile") + "]");
//        }
        this.collector = collector;
    }

    public void nextTuple() {
        String[] sentences = new String[]{"the cow jumped over the moon", "an apple a day keeps the doctor away",
                "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature"};
        String sentence = sentences[new Random().nextInt(sentences.length)];
        collector.emit(new Values(sentence));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("love"));
    }
}
