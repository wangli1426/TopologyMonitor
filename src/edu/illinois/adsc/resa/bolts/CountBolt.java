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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by Robert on 9/17/15.
 */
public class CountBolt extends BaseRichBolt {
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        long startTimeStamp = System.currentTimeMillis();
        TopologyLatencyRecord topologyLatencyRecord = (TopologyLatencyRecord) input.getValueByField("record");
        ComponentLatencyRecord componentLatencyRecord = topologyLatencyRecord.createComponentLatencyRecord("CountBolt", ComponentLatencyRecord.ComponentType.bolt);
        String word = input.getStringByField("word");
        if(counts.containsKey(word)) {
            counts.put(word, counts.get(word) + 1);
        } else {
            counts.put(word,1L);
        }

        componentLatencyRecord.setExecuteTime(System.currentTimeMillis() - startTimeStamp);
        topologyLatencyRecord.addNewTopologyLatencyRecord(componentLatencyRecord);

//        topologyLatencyRecord.prepareEmit(); prepareEmit is not necessary as this bolt is the last one in the actual data processing logic
        topologyLatencyRecord.notifyProcessLogicFinished();//notify TopologyLatencyRecord that the processing logic is completed!

        if (random.nextFloat()<0.02)
            outputCollector.emit(new Values(topologyLatencyRecord));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("record"));
    }

    private OutputCollector outputCollector;

    private Map<String, Long> counts = new HashMap<>();

    private Random random = new Random();
}
