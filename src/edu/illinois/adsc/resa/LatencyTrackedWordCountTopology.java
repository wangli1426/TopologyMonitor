package edu.illinois.adsc.resa;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import edu.illinois.adsc.resa.bolts.CountBolt;
import edu.illinois.adsc.resa.bolts.SentenceSplitBolt;
import edu.illinois.adsc.resa.bolts.TopologyLatnecyRecordPrintBolt;
import edu.illinois.adsc.resa.spouts.SentenceGeneratorSpout;
import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;
import java.io.*;

/**
 * Created by Robert on 9/17/15.
 */
public class LatencyTrackedWordCountTopology {

    public static void main(String[] args) throws Exception{

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("Sentence Generator", new SentenceGeneratorSpout(), 1);
        builder.setBolt("Split", new SentenceSplitBolt(), 3).shuffleGrouping("Sentence Generator");
        builder.setBolt("count", new CountBolt(), 3).fieldsGrouping("Split", new Fields("word"));
        builder.setBolt("printer", new TopologyLatnecyRecordPrintBolt(),1).shuffleGrouping("count");


        Config conf = new Config();

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
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
