/*
* Copyright (c) 2013-2015 Software AG, Darmstadt, Germany 
* and/or Software AG USA Inc., Reston, VA, USA, and/or 
* its subsidiaries and or/its affiliates and/or their 
* licensors.
* Use, reproduction, transfer, publication or disclosure 
* is prohibited except as specifically provided for in your 
* License Agreement with Software AG.
*/

package com.sag.tn.storm.stormmaven.spoutsources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.ximpleware.VTDGen;
import com.ximpleware.VTDNav;

public class AWSSQSSpoutSource implements ISpoutSource {

	private AmazonSQS sqs = new AmazonSQSClient(new ProfileCredentialsProvider().getCredentials());
	private String sqsQueueURL;
	private VTDGen vg = new VTDGen();
	private ReceiveMessageRequest receiveMessageRequest;

	public AWSSQSSpoutSource(String sqsQueueURL) {
		this.sqsQueueURL = sqsQueueURL;
		this.receiveMessageRequest = new ReceiveMessageRequest(this.sqsQueueURL);
	}
	
	@Override
	public List<TuplePair<VTDNav, String>> fetch() throws IOException {
		
		List<TuplePair<VTDNav, String>> list = new ArrayList<>();
		
		/*
		 * get duplicate check - ignore as it needs a centralized cache process delete
		 */
		try {
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			for(Message message: messages) {
				this.vg.setDoc(message.getBody().getBytes());
				this.vg.parse(true);
				list.add(new TuplePair<>(vg.getNav(), message.getMessageId()));
				this.vg.clear();
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	@Override
	public void close() throws Exception {
		this.sqs.shutdown();
	}
}
