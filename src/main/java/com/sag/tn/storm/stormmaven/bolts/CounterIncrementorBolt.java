/*
* Copyright (c) 2013-2014 Software AG, Darmstadt, Germany 
* and/or Software AG USA Inc., Reston, VA, USA, and/or 
* its subsidiaries and or/its affiliates and/or their 
* licensors.
* Use, reproduction, transfer, publication or disclosure 
* is prohibited except as specifically provided for in your 
* License Agreement with Software AG.
*/

package com.sag.tn.storm.stormmaven.bolts;

import static com.mongodb.client.model.Filters.eq;

import java.util.Arrays;
import java.util.Map;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

@SuppressWarnings("serial")
public class CounterIncrementorBolt implements IRichBolt {

	private OutputCollector collector;
	private Logger logger = LoggerFactory.getLogger(CounterIncrementorBolt.class);
	private MongoClient mClient;
	private MongoDatabase db;
	private MongoCollection<Document> coll;

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		MongoCredential credential = MongoCredential.createCredential((String)stormConf.get("MongoUser"), (String)stormConf.get("MongoDatabase"), ((String)stormConf.get("MongoPass")).toCharArray());
		this.mClient = new MongoClient(new ServerAddress((String)stormConf.get("MongoHost"), Integer.parseInt((String)stormConf.get("MongoPort"))), Arrays.asList(credential));
		this.db = this.mClient.getDatabase((String)stormConf.get("MongoDatabase"));
		this.coll = db.getCollection((String)stormConf.get("MongoColl"));
	}

	public void execute(Tuple input) {
		try {
			String docTypeId = (String)input.getValue(0);	//docTypeId whose count is to be incremented by 1
		
			//increment by 1
			this.logger.info("INCREMENTING DOCTYPE ID - com.sag.tn.storm.stormmaven.bolts.CounterIncrementorBolt: {} ", docTypeId);
			this.coll.updateOne(eq("docTypeId", docTypeId), new Document("$inc", new Document("execs", 1)));
			this.logger.info("INCREMENTED DOCTYPE ID - com.sag.tn.storm.stormmaven.bolts.CounterIncrementorBolt: {} ", docTypeId);

			this.collector.ack(input);
		} catch(Exception e) {
			e.printStackTrace();
		}

	}
	
	public void cleanup() {
		this.mClient.close();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
