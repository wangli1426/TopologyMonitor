package edu.illinois.adsc.resa.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import edu.illinois.adsc.resa.utils.TopologyLatencyRecord;

import java.util.Map;

/**
 * Created by Robert on 9/17/15.
 */
public class TopologyLatnecyRecordPrintBolt extends BaseRichBolt {
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        TopologyLatencyRecord topologyLatnecyRecordPrintBolt = (TopologyLatencyRecord) input.getValueByField("record");
        topologyLatnecyRecordPrintBolt.print();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
