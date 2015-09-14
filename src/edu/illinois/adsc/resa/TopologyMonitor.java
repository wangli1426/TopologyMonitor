package edu.illinois.adsc.resa;

import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.lang.String;
import java.util.List;
import java.util.Map;

public class TopologyMonitor {
    @Option(name = "--topology", aliases = {"-t"}, usage = "the name of the topology to monitor")
    private String topologyName; // MS

    @Option(name = "--help", aliases = {"-h"}, usage = "help")
    private boolean help = false;

    public static void main(String[] args) {
        new TopologyMonitor().realMain(args);
    }

    private void realMain(String [] args){

        CmdLineParser parser = new CmdLineParser(this);

        parser.setUsageWidth(80);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            help = true;
        }
        if (help) {
            parser.printUsage(System.err);
            System.err.println();
            return;
        }
        if (topologyName == null) {
            System.err.println("Topology name is missing. \nPrint -h to see how to specify the topology name.");
            return;
        }

        Map conf = Utils.readStormConfig();
        Nimbus.Client client = NimbusClient.getConfiguredClient(conf).getClient();
        try {
            TopologyInfo topologyInfo = client.getTopologyInfo(topologyName);
            List<ExecutorSummary> executorSummaries = topologyInfo.get_executors();
//            for(ExecutorSummary e: executorSummaries) {
//                System.out.print(e.get_component_id()+":");
//                System.out.println(e.get_stats());
//            }

        } catch (AuthorizationException e) {
            System.err.println(e.getCause());
            System.err.println(e.getStackTrace().toString());

        } catch ( NotAliveException e) {
            System.err.println(e.getCause());
            System.err.println(e.getStackTrace().toString());
        } catch (Exception e) {
            System.err.println(e.getCause());
            System.err.println(e.getStackTrace().toString());
        }
    }
}