package edu.illinois.adsc.resa;

import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import scala.collection.Set;

import java.lang.String;
import java.util.*;

public class TopologyMonitor {
    @Option(name = "--topology_id", aliases = {"-t"}, usage = "the ID of the topology to monitor")
    private String topologyName; // MS

    @Option(name = "--help", aliases = {"-h"}, usage = "help")
    private boolean help = false;

    @Option(name = "--print-system-component", aliases = {"-s"}, usage = "print system components")
    private boolean systemCompoment = false;

    static List<String> systemComponentNames = Arrays.asList("__acker");


    public static void main(String[] args) {
        new TopologyMonitor().realMain(args);
    }

    private void realMain(String[] args){

        CmdLineParser parser = new CmdLineParser(this);

        parser.setUsageWidth(800);
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

        NimbusClient Nimbusclient = NimbusClient.getConfiguredClient(conf);
        Nimbus.Client client = Nimbusclient.getClient();

        try {
            while(true) {
                Thread.sleep(1000);
                System.out.println("Topology name :" + topologyName);
                TopologyInfo topologyInfo = client.getTopologyInfo(topologyName);
                List<ExecutorSummary> executorSummaries = topologyInfo.get_executors();
                printSummary(executorSummaries);
            }

        } catch (AuthorizationException e) {
            System.err.println("AuthorizationException");
            System.err.println(e.getCause());
            System.err.println(e.getStackTrace().toString());

        } catch ( NotAliveException e) {
            System.err.print("NotAliveException");
                    System.err.println(e.getCause());
            System.err.println(e.getStackTrace());
        } catch (Exception e) {
            System.err.print("Exception");
            System.err.println(e.getCause());
            System.err.println(e.getStackTrace());
        }
    }

    private boolean passExecutorFilter(ExecutorSummary s) {
        if(systemComponentNames.contains(s.get_component_id()))
            return false;
        else
            return true;
    }

    private void printSummary(List<ExecutorSummary> executorSummaries) {

        TreeMap<String, ExecutorSummary> nameToSummaries = new TreeMap<String, ExecutorSummary>();

        for(ExecutorSummary s: executorSummaries) {
            if(passExecutorFilter(s))
                nameToSummaries.put(s.get_component_id()+"."+s.get_executor_info().get_task_start(),s);
        }

        for(ExecutorSummary e: nameToSummaries.values()) {
            System.out.print(e.get_host()+"."+e.get_component_id()+"."+e.get_executor_info().get_task_start()+":\t");
            System.out.format("%8.6f\n",e.get_stats().get_throughput().get("600").get("default"));
        }
    }
}