/*
* Copyright (c) 2013-2014 Software AG, Darmstadt, Germany 
* and/or Software AG USA Inc., Reston, VA, USA, and/or 
* its subsidiaries and or/its affiliates and/or their 
* licensors.
* Use, reproduction, transfer, publication or disclosure 
* is prohibited except as specifically provided for in your 
* License Agreement with Software AG.
*/

package com.sag.tn.storm.stormmaven.spouts;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.sag.tn.storm.stormmaven.spoutsources.AWSSQSSpoutSource;
import com.sag.tn.storm.stormmaven.spoutsources.XMLDirectorySpoutSource;
import com.sag.tn.storm.stormmaven.spoutsources.ISpoutSource;
import com.sag.tn.storm.stormmaven.spoutsources.TuplePair;
import com.ximpleware.VTDNav;

/*
 * Pushes a list of document objects (which are to be pushed) to the topology
 */
@SuppressWarnings("serial")
public final class DocumentFetcherSpout implements IRichSpout {

	private ISpoutSource source;
	private SpoutOutputCollector collector;
	private Logger logger = LoggerFactory.getLogger(DocumentFetcherSpout.class);
	
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		if("aws".equals((String)conf.get("SpoutSource"))) {
			this.source = new AWSSQSSpoutSource((String)conf.get("SQSURL"));
		} else {
			this.source = new XMLDirectorySpoutSource(".");
		}
		this.collector = collector;
	}
	
	public void close() {
		try {
			this.source.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	public void activate() {}

	public void deactivate() {}

	/*
	 * emit tuples
	 */
	public void nextTuple() {
		try {
			List<TuplePair<VTDNav, String>> list = this.source.fetch();
			if(list.size() == 0) {
				return;
			}
			for(TuplePair<VTDNav, String> tp: list) {
				this.logger.info("EMITTING MSG ID - com.sag.tn.storm.stormmaven.spouts.DocumentFetcherSpout: {} ", tp.getSecond());
				this.collector.emit(new Values(tp.getFirst()), tp.getSecond());
				this.logger.info("EMITTED MSG ID - com.sag.tn.storm.stormmaven.spouts.DocumentFetcherSpout: {} ", tp.getSecond());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void ack(Object msgId) {
		this.logger.info("OK - com.sag.tn.storm.stormmaven.spouts.DocumentFetcherSpoutOK: {} ", msgId);
	}

	public void fail(Object msgId) {
		this.logger.info("NOK - com.sag.tn.storm.stormmaven.spouts.DocumentFetcherSpoutOK: {} ", msgId);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("vnavobject"));

	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
