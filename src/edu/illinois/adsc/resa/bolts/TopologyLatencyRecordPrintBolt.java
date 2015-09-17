package edu.illinois.adsc.resa.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import edu.illinois.adsc.resa.utils.TopologyLatencyRecord;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

/**
 * Created by Robert on 9/17/15.
 */
public class TopologyLatencyRecordPrintBolt extends BaseRichBolt {

    public TopologyLatencyRecordPrintBolt(String outputFileName) throws IOException{
        fileName = outputFileName;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            fileWriter = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)));
        }
        catch (Exception e){

        }
    }

    @Override
    public void execute(Tuple input) {
        TopologyLatencyRecord topologyLatnecyRecordPrintBolt = (TopologyLatencyRecord) input.getValueByField("record");
//        topologyLatnecyRecordPrintBolt.print();
        fileWriter.append(topologyLatnecyRecordPrintBolt.toStringOutput());
        fileWriter.flush();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        TopologyLatencyRecord t = new TopologyLatencyRecord(100);
        System.out.print(t._payload.length);
    }

    private String fileName;
    private transient PrintWriter fileWriter;
}
