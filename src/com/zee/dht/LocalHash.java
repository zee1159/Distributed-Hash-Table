/***************************************************************************************\
 * Name			 : Zeeshan Aamir Khavas
 * Application	 : Distributed Hash Table P2P Application
 * Program		 : LocalHash.java
 * Description   : This class creates a concurrent hash map and performs the operations
 * 					PUT, GET and DEL on the concurrent hash map.
 * Date			 : 10/13/2015
 * @author Zee
\***************************************************************************************/

package com.zee.dht;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map.Entry;


public class LocalHash {

	/*Creating a concurrent hash map for storing the key-value pairs*/
	private ConcurrentHashMap<String, String> keyVal;

	/*Default Constructor to instantiate the hash map*/
	public LocalHash(){
		this.setKeyVal(new ConcurrentHashMap<String, String>());
	}

	/*Accessors and modifiers*/
	public ConcurrentHashMap<String, String> getKeyVal() {
		return keyVal;
	}

	public void setKeyVal(ConcurrentHashMap<String, String> keyVal) {
		this.keyVal = keyVal;
	}

	/* **********************************************************************
	 * Method Name 	:	put
	 * Parameters	:	String, String
	 * Returns		:	Boolean
	 * Description	:	Method to add a key-value pair in concurrent hash map
	 * **********************************************************************/
	public Boolean put(String key, String value) {
		getKeyVal().put(key, value);
		return true;
	}

	/* **********************************************************************
	 * Method Name 	:	get
	 * Parameters	:	String
	 * Returns		:	String
	 * Description	:	Method to retrieve a key-value pair from concurrent hash map
	 * **********************************************************************/
	public String get(String key) {
		String hashKeyVal;
		hashKeyVal = getKeyVal().get(key);
		return hashKeyVal;
	}

	/* **********************************************************************
	 * Method Name 	:	del
	 * Parameters	:	String
	 * Returns		:	Boolean
	 * Description	:	Method to remove a key-value pair from concurrent hash map
	 * **********************************************************************/
	public Boolean del(String key) {
		getKeyVal().remove(key);
		return true;
	}

}
