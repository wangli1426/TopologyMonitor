# TopologyMonitor
This is a Storm utility to display the instantaneous metrics of a topology. 

Before using this tool, you should make sure that your Storm Client is properly configured. This can be varified by using ```storm list``` command.

#####Example

Display the metrics for the topology with ID ```T1-3-1442239652```
```
storm jar target/topology-monitor-0.11.0-SNAPSHOT.jar edu.illinois.adsc.resa.TopologyMonitor -t T1-3-1442239652
```


#####Usage
Use -h option to print the detailed usage information:
```
storm jar target/topology-monitor-0.11.0-SNAPSHOT.jar edu.illinois.adsc.resa.TopologyMonitor -h
```
