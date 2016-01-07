---
services: hdinsight
platforms: dotnet
author: blackmist
---

# hdinsight-storm-dotnet-event-correlation

This example demonstrates how to use Apache Storm and a persistent data store (Apache HBase in this case,) to correlate pieces of data that arrive at different times. This example was written and tested using HDInsight on Microsoft Azure.

This example is based on tracking session state. When a session begins, a START event is sent through the topology. This event, the time it was sent, and the Session ID are stored in HBase.

Later, an END event is received. The topology uses the Session ID to look up the previously received START event and calculate the duration (time elapsed between the two,) and then stores the END event and duration to HBase.

The SessionInfo project is a basic HBase client application that can be used to create, delete, or do some basic time range queries against the table in HBase.

## Running this sample

1. Create a new HDInsight HBase cluster. This is where data will be stored by the Storm process.. See [Get started with HBase in HDInsight](https://azure.microsoft.com/en-us/documentation/articles/hdinsight-hbase-tutorial-get-started-linux/) for information on creating an HBase cluster.

2. Create a new HDInsight Storm cluster. This is where the Storm topology runs. See [Get started with Storm in HDInsight](https://azure.microsoft.com/en-us/documentation/articles/hdinsight-apache-storm-tutorial-get-started-linux/)

## Create the HBase table

1. Open the **SessionInfo** project in Visual Studio.

2. Right click on the project and select **Build** (to restore packages and such,) then right click again and select **Properties**.

3. In Properties, set the URL to your HBase cluster (https://clustername.azurehdinsight.net/,) and the admin user name and password for the cluster. You can leave the table name and column family entries as they are.

4. Run the SessionInfo project. When prompted, select 'c' to create a table.

##Configure and deploy the topology

1. Open the **CorrelationTopology** project in Visual Studio.

2. Right click on the project and select **Build** (to restore packages.) Then right click again and select **Properties**.

3. In Properties, set the URL to your HBase cluster (https://clustername.azurehdinsight.net/,) and the admin user name and password for the cluster. You can leave the table name and column family entries as they are.

3. Right click the project and select **Submit to Storm on HDInsight**. You may have to authenticate to your Azure subscription here. When the dialog appears, and lists your clusters, select the Storm cluster to deploy to.

	After deployment, the Topology viewer will appear. At this point the topology is running. Give it a few seconds to start generating events.

##Query data

1. Go back to the running SessionInfo project. From here, select 's' to see START events that have been logged.

2. When prompted, enter a start time of around the time the topology started on the Storm cluster. Use a time format of HH:MMam or pm. 
	
3. When prompted, enter an end time of the current time, or some time in the future to make sure you have a time range of a couple minutes.
	
	The utility will search HBase for START events in the given time span and return a list of events.
	
Searching for END events works the same how, however END events happen between 1 and 5 minutes after the START event. Once END events are fired, more START events will occur. The topology will keep generating these events as long as you leave it running

##Cleanup

After you have finished running this, you can stop the topology from the topology viewer in Visual Studio. Select the topology, and then use the **Kill** button to stop the topology.

To remove the table from HBase, select 'd' from the SessionInfo application.

If you are finished with the HBase and Storm clusters, you should delete both clusters from the Azure portal, as you will be charged for every hour that the clusters exist.

## About the code

The Storm topology uses the following components:

* Sessions - session.cs defines the session object used in this example. Sessions have the following properties:

    - id - a randomly generated GUID
    - state - 'START' or 'END'
    - emitted - the last date/time that the session was emitted
    - timeout - how long (in minutes,) after a session has been 'START'ed, that it is eligible to be 'END'ed.

* Spout - spout.cs generates 100 sessions, which all begin life in the 'START' state. Each session is also assigned a random timeout.

    Once the spout emits the session into the topology, it then waits until sessions start timing out. When this happens, it sets the state to 'END' and emits the session. Then the session's slot is recycled with a new session.

* HBase lookup bolt - hbaselookupbolt.cs is used to calculate the duration of a session. If the incoming session (from the spout) is 'START', it gives a duration of 0 and passes it through. If it is 'END', it will look up the corresponding 'START' from HBase and calculate the duration, then emit the 'END' message with the duration value.

    Note that this is a perfect world scenario that assumes that for every 'END' received, there was a 'START'. And that the 'END' arrives after the 'START'. In the real world, you may have situations where the events arrive out of order, or are duplicated.

* HBase bolt - hbasebolt.cs writes the incoming session (from HBase lookup bolt,) to HBase.
    
## More information

For more information on this example, see [Correlate events with Storm on HDInsight](https://azure.microsoft.com/en-us/documentation/articles/hdinsight-storm-correlation-topology/)

For other examples for Storm on HDInsight, see [Example topologies for Storm on HDInsight](https://azure.microsoft.com/en-us/documentation/articles/hdinsight-storm-example-topology/)