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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.sag.tn.storm.stormmaven.vtd.Util;
import com.ximpleware.NavException;
import com.ximpleware.VTDNav;

@SuppressWarnings("serial")
public class DocumentTypeIdentifierBolt implements IRichBolt {

	private OutputCollector collector;
	private Logger logger = LoggerFactory.getLogger(DocumentTypeIdentifierBolt.class);
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

	/*
	 * get parsed VTDNav objects - each object for 1 XML input
	 * do matching here with document types from doctype store
	 */
	public void execute(Tuple input) {
		VTDNav vn = (VTDNav)input.getValue(0);	//vn object to be processed
		try {
			String rootTag = Util.getRootTag(vn);
			
			//make a mongo query to get document type id from root tag name
			MongoCursor<Document> cursor = this.coll.find(eq("rootTag", rootTag)).iterator();
			while(cursor.hasNext()) {
			    Document document = cursor.next();
			    List<Tuple> list = new ArrayList<>();
				list.add(input);
				this.logger.info("EMITTING DOCTYPE ID - com.sag.tn.storm.stormmaven.bolts.DocumentTypeIdentifierBolt: {} ", (String)document.get("docTypeId"));
				this.collector.emit(list, new Values((String)document.get("docTypeId")));
				this.logger.info("EMITTED DOCTYPE ID - com.sag.tn.storm.stormmaven.bolts.DocumentTypeIdentifierBolt: {} ", (String)document.get("docTypeId"));
				break;
			}
			this.collector.ack(input);
		} catch (NavException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void cleanup() {
		this.mClient.close();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("docTypeid"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
