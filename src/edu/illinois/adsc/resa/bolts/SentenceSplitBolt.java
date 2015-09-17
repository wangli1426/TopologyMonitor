package edu.illinois.adsc.resa.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.illinois.adsc.resa.utils.ComponentLatencyRecord;
import edu.illinois.adsc.resa.utils.TopologyLatencyRecord;

import java.util.Map;

/**
 * Created by Robert on 9/17/15.
 */
public class SentenceSplitBolt extends BaseRichBolt {
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        long startTimeStamp = System.currentTimeMillis();
        TopologyLatencyRecord topologyLatencyRecord = (TopologyLatencyRecord)input.getValueByField("record");

        ComponentLatencyRecord componentLatencyRecord = topologyLatencyRecord.createComponentLatencyRecord("SentenceSplitBolt", ComponentLatencyRecord.ComponentType.bolt);

        String sentence = input.getStringByField("sentence");
        String[] words = sentence.split(" ");

        for(String w: words) {
            TopologyLatencyRecord currentTopologyLatencyRecord;
            currentTopologyLatencyRecord=new TopologyLatencyRecord(topologyLatencyRecord);

            componentLatencyRecord.setExecuteTime(System.currentTimeMillis() - startTimeStamp);
            currentTopologyLatencyRecord.addNewTopologyLatencyRecord((ComponentLatencyRecord)componentLatencyRecord.clone());
            currentTopologyLatencyRecord.prepareEmit();
            outputCollector.emit(new Values(currentTopologyLatencyRecord, w));
            break;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("record","word"));

    }

    private OutputCollector outputCollector;
}
