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

/**
 * @author MABH
 * A 2-Tuple
 */
public final class TuplePair<A, B> {
	private A a;
	private B b;
	
	public TuplePair(A a, B b) {
		this.a = a;
		this.b = b;
	}
	
	public A getFirst() {
		return this.a;
	}
	
	public B getSecond() {
		return this.b;
	}	
}
