package edu.illinois.adsc.resa.utils;

import java.io.Serializable;
import java.util.Vector;

/**
 * This class contains variable that is used to facilitate tracking the latency of a
 * storm topology and storing the latency information.
 */
public class TopologyLatencyRecord implements Serializable {


    public TopologyLatencyRecord(int payloadSize) throws IllegalArgumentException {
        int payloadLength = payloadSize/(Long.SIZE/8);
        if (payloadLength < 1)
            throw new IllegalArgumentException("pay load size should be larger than 8");
        _startTimeStamp = System.currentTimeMillis();
        _payload = new Long[payloadSize/(Long.SIZE/8)];
    }

    public TopologyLatencyRecord(TopologyLatencyRecord origin) {
        this._executeLatency = origin._executeLatency;
        this._lastSendTimeStamp = origin._lastSendTimeStamp;
        this._startTimeStamp = origin._startTimeStamp;
        this.componentLatencyRecords = (Vector<ComponentLatencyRecord>)origin.componentLatencyRecords.clone();
//        this._payload = origin._payload.clone();
    }

    public void prepareEmit() {
        _lastSendTimeStamp = System.currentTimeMillis();
    }

    public ComponentLatencyRecord createComponentLatencyRecord(String componentName, ComponentLatencyRecord.ComponentType type) throws IllegalArgumentException {
        if(type == ComponentLatencyRecord.ComponentType.bolt)
            return new ComponentLatencyRecord(componentName, type, _lastSendTimeStamp);
        else
            return new ComponentLatencyRecord(componentName, type);
    }

    public void addNewTopologyLatencyRecord(ComponentLatencyRecord record) {
        componentLatencyRecords.add(record);
    }

    public void eraseLastTopologyLatencyRecord() {
        componentLatencyRecords.remove(componentLatencyRecords.size() - 1);
    }

    public void notifyProcessLogicFinished() {
        _executeLatency = System.currentTimeMillis() - _startTimeStamp;
    }

    public void print() {
        System.out.println("\t\t"+"receiving latency\t\t"+"execute latency");
        for(ComponentLatencyRecord record: componentLatencyRecords) {
            System.out.print(record.componentName+":\t\t");
            System.out.println(record.receivingDelay + "\t\t" + record.executeTime);
        }
        System.out.println("total execution delay:"+_executeLatency+"ms");
    }

    public String toStringOutput() {
        String str="";
        str += "\t\t"+"receiving latency\t\t"+"execute latency\n";
        for(ComponentLatencyRecord record: componentLatencyRecords) {
            str += record.componentName+":\t\t";
            str += record.receivingDelay + "\t\t" + record.executeTime + "\n";
        }
        str += "total execution delay:"+_executeLatency+"ms\n";
        return str;
    }

    Vector<ComponentLatencyRecord> componentLatencyRecords = new Vector<>();

    public static void main(String[] args) throws InterruptedException {
        TopologyLatencyRecord topologyLatencyRecord = new TopologyLatencyRecord(1000);

        ComponentLatencyRecord spoutLatencyRecord = topologyLatencyRecord.createComponentLatencyRecord("spout", ComponentLatencyRecord.ComponentType.spout);

        Thread.sleep(100);
        spoutLatencyRecord.setExecuteTime(100);
        topologyLatencyRecord.addNewTopologyLatencyRecord(spoutLatencyRecord);
        topologyLatencyRecord.prepareEmit();

        //===============================
        Thread.sleep(1000);

        ComponentLatencyRecord boltLatencyRecord = topologyLatencyRecord.createComponentLatencyRecord("bolt1", ComponentLatencyRecord.ComponentType.bolt);
        System.out.println("Delay:" + boltLatencyRecord.receivingDelay);
        Thread.sleep(200);
        boltLatencyRecord.setExecuteTime(200);
        topologyLatencyRecord.addNewTopologyLatencyRecord(boltLatencyRecord);
        topologyLatencyRecord.prepareEmit();
    }

    private long _lastSendTimeStamp;

    private long _startTimeStamp;

    public long _executeLatency;

    public Long[] _payload;
}
