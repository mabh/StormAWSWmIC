/*
* Copyright (c) 2013-2014 Software AG, Darmstadt, Germany 
* and/or Software AG USA Inc., Reston, VA, USA, and/or 
* its subsidiaries and or/its affiliates and/or their 
* licensors.
* Use, reproduction, transfer, publication or disclosure 
* is prohibited except as specifically provided for in your 
* License Agreement with Software AG.
*/

package com.sag.tn.storm.stormmaven.main;

import org.apache.thrift7.TException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import com.sag.tn.storm.stormmaven.bolts.CounterIncrementorBolt;
import com.sag.tn.storm.stormmaven.bolts.DocumentTypeIdentifierBolt;
import com.sag.tn.storm.stormmaven.spouts.DocumentFetcherSpout;
import com.sag.tn.storm.stormmaven.vtd.Util;

public final class DocumentTopologyMain {
	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, TException {
		
		int nSupervisors = Util.getNumberOfSupervisors(ConfigReader.getInstance().get("nimbusHost"));
		int workersMultiple = 3;	//worker processes per supervisor node
		int executorsMultiple = 3;	//bolt executors per worker process
		int nWorkers = workersMultiple * nSupervisors;
		int nExecutors = executorsMultiple * nWorkers;
		
		
		
		
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("fetcher", new DocumentFetcherSpout(), nExecutors);
		builder.setBolt("identifier", new DocumentTypeIdentifierBolt(), nExecutors).shuffleGrouping("fetcher");
		builder.setBolt("incrementor", new CounterIncrementorBolt(), nExecutors).shuffleGrouping("identifier");
		
		
		
		
		/*
		 * create conf
		 */
		Config conf = new Config();
		conf.put("SpoutSource", ConfigReader.getInstance().get("SpoutSource"));
		conf.put("SQSURL", 
				System.getProperty(ConfigReader.getInstance().get("AWSServerURLProperty")) + "/" +
				System.getProperty(ConfigReader.getInstance().get("AWSAccountProperty")) + "/" + 
				System.getProperty(ConfigReader.getInstance().get("SQSQueueProperty"))
		);
		conf.put("MongoHost", System.getProperty(ConfigReader.getInstance().get("MongoHostProperty")));
		conf.put("MongoPort", System.getProperty(ConfigReader.getInstance().get("MongoPortProperty")));
		conf.put("MongoUser", System.getProperty(ConfigReader.getInstance().get("MongoUserProperty")));
		conf.put("MongoPass", System.getProperty(ConfigReader.getInstance().get("MongoPassProperty")));
		conf.put("MongoDatabase", System.getProperty(ConfigReader.getInstance().get("MongoDBProperty")));
		conf.put("MongoColl", System.getProperty(ConfigReader.getInstance().get("MongoCollProperty")));
		conf.setNumWorkers(nWorkers);
		conf.setDebug(false);
		conf.registerSerialization(com.ximpleware.VTDNav.class, com.sag.tn.storm.stormmaven.vtd.VSerializer.class);

		
		
		
		/*
		 * submit topology to LocalCluster to run it
		 */
		if("local".equalsIgnoreCase(args[0])) {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("TNStorm", conf, builder.createTopology());
			Thread.sleep(20000);
			localCluster.shutdown();
		} else {
			StormSubmitter.submitTopology("TNStorm", conf, builder.createTopology());
		}
	}
}
