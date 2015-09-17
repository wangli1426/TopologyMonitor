package edu.illinois.adsc.resa.utils;

import org.junit.Test;
import junit.framework.TestCase;

import java.lang.InterruptedException;

/**
 * Unit test for RateTracker
 */
public class TopologyLatencyRecordTest extends TestCase {

    @Test
    public void testEclipsedAllWindows() throws InterruptedException{
        TopologyLatencyRecord topologyLatencyRecord = new TopologyLatencyRecord(1000);

        ComponentLatencyRecord spoutLatencyRecord = topologyLatencyRecord.createComponentLatencyRecord("spout", ComponentLatencyRecord.ComponentType.spout);

        Thread.sleep(100);
        spoutLatencyRecord.setExecuteTime(100);
        topologyLatencyRecord.addNewTopologyLatencyRecord(spoutLatencyRecord);
        topologyLatencyRecord.prepareEmit();

        //===============================
        Thread.sleep(1000);

        ComponentLatencyRecord boltLatencyRecord = topologyLatencyRecord.createComponentLatencyRecord("bolt1", ComponentLatencyRecord.ComponentType.bolt);
        Thread.sleep(200);
        boltLatencyRecord.setExecuteTime(200);
        topologyLatencyRecord.addNewTopologyLatencyRecord(boltLatencyRecord);
        topologyLatencyRecord.prepareEmit();

    }
}
