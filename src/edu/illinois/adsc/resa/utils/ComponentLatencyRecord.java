package edu.illinois.adsc.resa.utils;


import java.io.Serializable;

/**
 * Created by Robert on 9/17/15.
 */
public class ComponentLatencyRecord implements Serializable, Cloneable{

    public enum ComponentType {spout,bolt};

    public ComponentLatencyRecord(ComponentLatencyRecord r) {
        this.executeTime = r.executeTime;
        this.componentName = r.componentName;
        this.receivingDelay = r.receivingDelay;
        this.componentType = r.componentType;
    }

    public ComponentLatencyRecord(String name, ComponentType type, long lastSendingTimeStamp) throws IllegalArgumentException{
        if ( type == ComponentType.spout )
            throw new IllegalArgumentException("cannot specify lastSendingTimeStamp for spout!");
        componentName = name;
        componentType = type;
        receivingDelay = System.currentTimeMillis() - lastSendingTimeStamp;
    }

    public ComponentLatencyRecord(String name, ComponentType type) throws IllegalArgumentException{
        if ( type == ComponentType.bolt )
            throw new IllegalArgumentException("must specify lastSendingTimeStamp for spout!");
        componentName = name;
        componentType = type;
        receivingDelay = 0;
    }

    public void setExecuteTime(long time) {
        executeTime = time;
    }

    public Object clone(){
        return new ComponentLatencyRecord(this);
    }


    public long executeTime;

    /* the duration from the time this record is sent by the upstream component to the time
     this record is received by the current component.
      */
    public long receivingDelay;

    private ComponentType componentType;
    public String componentName;

}
