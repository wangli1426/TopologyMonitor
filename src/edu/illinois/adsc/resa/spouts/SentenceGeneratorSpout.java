package edu.illinois.adsc.resa.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import edu.illinois.adsc.resa.utils.ComponentLatencyRecord;
import edu.illinois.adsc.resa.utils.TopologyLatencyRecord;

import java.util.Map;
import java.util.Random;

/**
 * Created by Robert on 9/17/15.
 */
public class SentenceGeneratorSpout extends BaseRichSpout {
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("record","sentence"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        outputCollector = collector;
        random = new Random();
    }

    @Override
    public void nextTuple() {
        try {
            Thread.sleep(100);
        }
        catch (Exception e){

        }
        long startTimeStamp = System.currentTimeMillis();
        TopologyLatencyRecord topologyLatencyRecord = new TopologyLatencyRecord();
        ComponentLatencyRecord componentLatencyRecord = topologyLatencyRecord.createComponentLatencyRecord("SentenceGeneratorSpout", ComponentLatencyRecord.ComponentType.spout);
        String sentence = sentences[random.nextInt(sentences.length)];
        componentLatencyRecord.setExecuteTime(System.currentTimeMillis() - startTimeStamp);
        topologyLatencyRecord.addNewTopologyLatencyRecord(componentLatencyRecord);
                topologyLatencyRecord.prepareEmit();
        outputCollector.emit(new Values(topologyLatencyRecord,sentence));
    }

    private SpoutOutputCollector outputCollector;

    static private String[] sentences = {"Storm uses tuples as its data model",
            "A tuple is a named list of values",
            "A field in a tuple can be an object of any type",
            "Out of the box",
            "Storm supports all the primitive types",
            "strings and byte arrays as tuple field values", "To use an object of another type",
            "you just need to implement a serializer for the type"};
    private Random random;
}
