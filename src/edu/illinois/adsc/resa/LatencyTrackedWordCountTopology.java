package edu.illinois.adsc.resa;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import edu.illinois.adsc.resa.bolts.CountBolt;
import edu.illinois.adsc.resa.bolts.SentenceSplitBolt;
import edu.illinois.adsc.resa.bolts.TopologyLatencyRecordPrintBolt;
import edu.illinois.adsc.resa.spouts.SentenceGeneratorSpout;

/**
 * Created by Robert on 9/17/15.
 */
public class LatencyTrackedWordCountTopology {

    public static void main(String[] args) throws Exception{

        if(args.length < 1) {
            throw new IllegalArgumentException("You should specify output file!");
        }

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("Sentence Generator", new SentenceGeneratorSpout(Integer.valueOf(args[1])), 1);
        builder.setBolt("Split", new SentenceSplitBolt(), 3).shuffleGrouping("Sentence Generator");
        builder.setBolt("count", new CountBolt(), 3).fieldsGrouping("Split", new Fields("word"));
        builder.setBolt("printer", new TopologyLatencyRecordPrintBolt(args[0]),1).shuffleGrouping("count");


        Config conf = new Config();

        if (args != null && args.length > 2) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[2], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

            Thread.sleep(100000);

            cluster.shutdown();
        }


    }

}
