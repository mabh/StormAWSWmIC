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

import static com.mongodb.client.model.Filters.eq;

import java.util.Arrays;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Main {
	public static void main(String[] args) {

		/*AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.",
                    e);
        }
		
        
        AmazonSQS sqs = new AmazonSQSClient(credentials);
        
        sqs.sendMessage(new SendMessageRequest("https://sqs.us-west-2.amazonaws.com/238337343154/b2baaSTestQueue", "0This is my message text."));
        sqs.sendMessage(new SendMessageRequest("https://sqs.us-west-2.amazonaws.com/238337343154/b2baaSTestQueue", "1This is my message text."));
        sqs.sendMessage(new SendMessageRequest("https://sqs.us-west-2.amazonaws.com/238337343154/b2baaSTestQueue", "2This is my message text."));
        sqs.sendMessage(new SendMessageRequest("https://sqs.us-west-2.amazonaws.com/238337343154/b2baaSTestQueue", "3This is my message text."));
        sqs.sendMessage(new SendMessageRequest("https://sqs.us-west-2.amazonaws.com/238337343154/b2baaSTestQueue", "4This is my message text."));
        
        
        // Receive messages
        System.out.println("Receiving messages from MyQueue.\n");
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest("https://sqs.us-west-2.amazonaws.com/238337343154/b2baaSTestQueue");
        
        for(int i = 0; i < 7; i++) {
        
	        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
	        
	        System.out.println(messages.size());
	        
	        for (Message message : messages) {
	            System.out.println("  Message");
	            System.out.println("    MessageId:     " + message.getMessageId());
	            System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
	            System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
	            System.out.println("    Body:          " + message.getBody());
	            for (Entry<String, String> entry : message.getAttributes().entrySet()) {
	                System.out.println("  Attribute");
	                System.out.println("    Name:  " + entry.getKey());
	                System.out.println("    Value: " + entry.getValue());
	            }
	            
	            sqs.deleteMessage(new DeleteMessageRequest("https://sqs.us-west-2.amazonaws.com/238337343154/b2baaSTestQueue", message.getReceiptHandle()));
	        }
        }*/
		
		MongoCredential credential = MongoCredential.createCredential("kunalkej", "ti-test-mongo-db", "kunalkej".toCharArray());
		MongoClient mClient = new MongoClient(new ServerAddress("ds033760.mongolab.com", 33760), Arrays.asList(credential));
		MongoDatabase db = mClient.getDatabase("ti-test-mongo-db");
		MongoCollection<Document> coll = db.getCollection("document-type");
		
		/*MongoCursor<Document> cursor = coll.find(eq("rootTag", "IDataXMLCoder")).iterator();
		while(cursor.hasNext()) {
		    Document document = cursor.next();
			System.out.println((String)document.get("docTypeId"));
			break;
		}*/
		
		coll.updateOne(eq("docTypeId", "f0cf1e95-9406-44dc-930f-8de9aae8ccaf123"),
		          new Document("$inc", new Document("execs", 100)));
		
		System.out.println("updated...");
		
		mClient.close();
		
	}
}
