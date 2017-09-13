package com.wpz.storm.logAnalyze;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;

/**
 * @Author: wpz
 * @Desctription:
 * @Date: Created in 2017/9/8 17:21
 */
public class ReaderSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        //  String uri = "hdfs://master:9000/storm/m.txt";
        String uri = "hdfs://192.168.32.143:9000/storm/app.log";
        InputStream in = null;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            in = fs.open(new Path((uri)));
            BufferedReader br = new BufferedReader((new InputStreamReader(in)));
            String line = null;
            while (null != (line = br.readLine())) {
                _collector.emit(new Values(line));
                Utils.sleep(100);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ip"));
    }
}
