/*
* Copyright (c) 2013-2015 Software AG, Darmstadt, Germany 
* and/or Software AG USA Inc., Reston, VA, USA, and/or 
* its subsidiaries and or/its affiliates and/or their 
* licensors.
* Use, reproduction, transfer, publication or disclosure 
* is prohibited except as specifically provided for in your 
* License Agreement with Software AG.
*/

package com.sag.tn.storm.stormmaven.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public final class ConfigReader {

	private static ConfigReader instance = new ConfigReader();
	private JSONObject configJsonObject = null;
	
	private ConfigReader() {
		try {
			this.configJsonObject = (JSONObject)JSONValue.parse(new FileReader(new File("config.json")));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static ConfigReader getInstance() {
		return instance;
	}
	
	public String get(final String key) {
		return (String)((JSONObject)configJsonObject.get("config")).get(key);
	}

	public static void main(String[] args) {
		System.out.println(ConfigReader.getInstance().get("supervisorHost"));
	}
}
