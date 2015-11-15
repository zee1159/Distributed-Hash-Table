/***************************************************************************************\
 * Name			 : Zeeshan Aamir Khavas
 * Application	 : Distributed Hash Table P2P Application
 * Program		 : Manager.java
 * Description   : This class is a runnable thread. It is instantiated for each client/server
 * 				   connection requests. It handles all the client/server requests by calling
 * 				   their respective methods.
 * Date			 : 10/13/2015
 * @author Zee
\***************************************************************************************/

package com.zee.dht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Hashtable;
import java.util.Map.Entry;


public class Manager implements Runnable {
	private int set;
	private Socket myClient;
	private String key;
	LocalHash hash;
	DataInputStream clientIn;
	DataOutputStream clientOut;

	/*Hash tables to store the Socket IO streams of the other connected servers*/
	private Hashtable<Integer, DataOutputStream> servers = new Hashtable<Integer, DataOutputStream>();
	private Hashtable<Integer, DataInputStream> serversIn = new Hashtable<Integer, DataInputStream>();

	/*Default constructor*/
	public Manager(){

	}

	/* **********************************************************************
	 * Method Name 	:	Manager
	 * Parameters	:	Socket, String, LocalHash
	 * Returns		:	void
	 * Description	:	Parameterized constructor that will set the
	 * 					client/server values
	 * **********************************************************************/
	public Manager(Socket myClient, String key, LocalHash hash) {
		this.myClient = myClient;
		this.key = key;
		this.hash = hash;
	}

	/* **********************************************************************
	 * Method Name 	:	run
	 * Parameters	:	No parameters
	 * Returns		:	void
	 * Description	:	This method is will be executed first in a thread
	 * 					execution.
	 * **********************************************************************/
	@Override
	public void run() {
		try {
			set = 0;
			processClient();
		}
		catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			System.out.println("Peer [ " + (myClient.getInetAddress()).getHostAddress() + ":" + myClient.getPort() + " ] disconnected !");
			//e.printStackTrace();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			System.out.println("Peer [ " + (myClient.getInetAddress()).getHostAddress() + ":" + myClient.getPort() + " ] disconnected !");
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			System.out.println("Peer [ " + (myClient.getInetAddress()).getHostAddress() + ":" + myClient.getPort() + " ] disconnected !");
			//e.printStackTrace();
		}
	}

	/* **********************************************************************
	 * Method Name 	:	processCLient
	 * Parameters	:	No parameters
	 * Returns		:	void
	 * Description	:	Method to accept requests from client/server and call
	 * 					respective methods for processing.
	 * **********************************************************************/
	private void processClient() throws NoSuchAlgorithmException, NumberFormatException, InterruptedException {
		String message;

		try {
			//Instantiating the socket IO stream objects for the thread
			clientIn = new DataInputStream(myClient.getInputStream());
			clientOut = new DataOutputStream(myClient.getOutputStream());

			while(myClient.isBound()){		//till the client connection with server is bound, it will listen for requests from client
				message = clientIn.readUTF();

				//Calling request handler based on messages from SERVER or CLIENT
				if(message.equalsIgnoreCase("CLIENT")){
					clientHandler(clientIn, clientOut);
				}
				else if(message.equalsIgnoreCase("SERVER")){
					serverHandler(clientIn, clientOut);
				}

			}

		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Peer [ " + (myClient.getInetAddress()).getHostAddress() + ":" + myClient.getPort() + " ] disconnected !");
		}

	}

	/* **********************************************************************
	 * Method Name 	:	clientHandler
	 * Parameters	:	DataInputStream, DataOutputStream
	 * Returns		:	void
	 * Description	:	Method to process requests(GET, PUT, DEL) from client
	 * 					P2P nodes
	 * **********************************************************************/
	private void clientHandler(DataInputStream clientIn, DataOutputStream clientOut) throws IOException, NoSuchAlgorithmException, NumberFormatException, InterruptedException {
		/*Check if all the servers are connected,
		 *if not call method to connect all servers.
		 */
		if(set == 0){
			connectServers(key);
		}


		String message = clientIn.readUTF();

		//"PUT" request will add the key-value pairs received from client to hash table
		if(message.equalsIgnoreCase("PUT")){
			int size, val;
			size = clientIn.readInt();
			String[] pairs = new String[2];
			String hashString, result;
			boolean res;

			informServer(size, message);	//Informs all server to prepare for PUT request

			for(int i = 0; i < size; i++){
				pairs = clientIn.readUTF().split(":");

				val = Integer.parseInt(pairs[0]) % 8;	//Calculates the mod value for the key received
				hashString = calculateSha(pairs[0]);	//Calculates the hash value of the key received

				if(val == Integer.parseInt(key) || (Integer.parseInt(key) - val) == 8){
					res = hash.put(hashString.toString(), pairs[1]);	//Adds the key value pair to local hash table of server
					if(res == true)
						clientOut.writeUTF("TRUE");
					else
						clientOut.writeUTF("FALSE");
				}
				else{
					//Sends the key-value pair to the other servers for storing it in their local hash table
					result = connectToServer(val, hashString.toString(), pairs[1], message, mapper(val), mapperIn(val) );
					clientOut.writeUTF(result);
				}

			}
		}
		//"GET" request will retrieve the key-value pairs for the key received from client
		else if(message.equalsIgnoreCase("GET")){
			int size, val;
			String getVal, res, hashString, hashKey;
			size = clientIn.readInt();

			informServer(size, message);	//Informs all server to prepare for GET request

			for(int i = 0; i < size; i++){
				hashKey = clientIn.readUTF();

				val = Integer.parseInt(hashKey) % 8;	//Calculates the mod value for the key received
				hashString = calculateSha(hashKey);		//Calculates the hash value of the key received

				if(val == Integer.parseInt(key) || (Integer.parseInt(key) - val) == 8){
					getVal = hash.get(hashString.toString());	//Retrieves key-value pair from the local hash table
					clientOut.writeUTF(getVal);
				}
				else{
					//Sends the key-value pair to the other servers for storing it in their local hash table
					res = connectToServer(val, hashString.toString(), null, message, mapper(val), mapperIn(val));
					clientOut.writeUTF(res);
				}
			}

		}
		//"DEL" request will remove the key-value pairs for the key received from client
		else if(message.equalsIgnoreCase("DEL")){
			String hashKey;

			String hashString, result;
			int val, size;
			boolean res;

			size = clientIn.readInt();

			informServer(size, message);	//Informs all server to prepare for GET request

			for(int i = 0; i < size; i++){
				hashKey = clientIn.readUTF();
				val = Integer.parseInt(hashKey) % 8;	//Calculates the mod value for the key received
				hashString = calculateSha(hashKey);		//Calculates the hash value of the key received

				if(val == Integer.parseInt(key) || (Integer.parseInt(key) - val) == 8){
					res = hash.del(hashString.toString());	//Deletes key-value pair from the local hash table
					if(res == true)
						clientOut.writeUTF("TRUE");
					else
						clientOut.writeUTF("FALSE");
				}
				else{
					//Sends the key to the other servers for removing key-value pair from their local hash table
					result = connectToServer(val, hashString.toString(), null, message, mapper(val), mapperIn(val));
					clientOut.writeUTF(result);
				}
			}
		}
		//"CLOSE" request will disconnects the client from server
		else if(message.equalsIgnoreCase("CLOSE")){
			myClient.close();
			informServer(0, message);
			System.out.println("Peer [ " + (myClient.getInetAddress()).getHostAddress() + ":" + myClient.getPort() + " ] disconnected !");
		}
	}

	/* **********************************************************************
	 * Method Name 	:	calculateSha
	 * Parameters	:	String
	 * Returns		:	String
	 * Description	:	Method to hash value for the parameter passed using
	 * 					SHA-1 algorithm
	 * **********************************************************************/
	private String calculateSha(String locKey) throws NoSuchAlgorithmException {
		MessageDigest mDigest = MessageDigest.getInstance("SHA1");
        byte[] result = mDigest.digest(locKey.getBytes());
        StringBuffer shRes = new StringBuffer();
        for (int i = 0; i < result.length; i++) {
        	shRes.append(Integer.toString((result[i] & 0xff) + 0x100, 16).substring(1));
        }
        return (shRes.toString());
	}

	/* **********************************************************************
	 * Method Name 	:	informServer
	 * Parameters	:	int, String
	 * Returns		:	void
	 * Description	:	Method to sending all other servers request that the
	 * 					upcoming requests are either PUT or GET or DEL
	 * **********************************************************************/
	private void informServer(int size, String message) throws IOException {
		DataOutputStream servOut;
		size = size / 8;
		for(Entry<Integer, DataOutputStream> entry: servers.entrySet()){
			servOut = entry.getValue();
			servOut.writeUTF("SERVER");
			servOut.writeUTF(message);
			servOut.writeInt(size);
		}
	}

	/* **********************************************************************
	 * Method Name 	:	serverHandler
	 * Parameters	:	DataInputStream, DataOutputStream
	 * Returns		:	void
	 * Description	:	Method to sending all other servers request that the
	 * 					upcoming requests are either PUT or GET or DEL
	 * **********************************************************************/
	private void serverHandler(DataInputStream clientIn, DataOutputStream clientOut) throws IOException {
		String message = clientIn.readUTF();
		String[] keyVal;
		String hashString, res;
		int size;
		boolean response;

		//"PUT" request will add the key-value pairs received from server to hash table
		if(message.equalsIgnoreCase("PUT")){
			size = clientIn.readInt();
			for(int i = 0; i < size; i++){
				keyVal = clientIn.readUTF().split(",");
				response = hash.put(keyVal[0], keyVal[1]);	//Adds the key value pair to local hash table of server
				if(response == true)
					clientOut.writeUTF("TRUE");
				else
					clientOut.writeUTF("FALSE");
			}
		}
		//"GET" request will retrieve the key-value pairs for the key received from client
		else if(message.equalsIgnoreCase("GET")){
			size = clientIn.readInt();
			for(int i = 0; i < size; i++){
				hashString = clientIn.readUTF();
				res = hash.get(hashString);		//Retrieves key-value pair from the local hash table
				clientOut.writeUTF(res);
			}
		}
		//"DEL" request will remove the key-value pairs for the key received from client
		else if(message.equalsIgnoreCase("DEL")){
			size = clientIn.readInt();
			for(int i = 0; i < size; i++){
				hashString = clientIn.readUTF();
				response = hash.del(hashString);	//Deletes key-value pair from the local hash table
				if(response == true)
					clientOut.writeUTF("TRUE");
				else
					clientOut.writeUTF("FALSE");
			}
		}
		//"CLOSE" request will disconnects the other servers connected to this server
		else if(message.equalsIgnoreCase("DEL")){
			myClient.close();
			System.out.println("Peer [ " + (myClient.getInetAddress()).getHostAddress() + ":" + myClient.getPort() + " ] disconnected !");
		}
	}

	/* **********************************************************************
	 * Method Name 	:	connectToServer
	 * Parameters	:	int, String, String, String, DataInputStream, DataOutputStream
	 * Returns		:	String
	 * Description	:	Method to sending all other servers key-value pairs for
	 * 					operations PUT or GET or DEL
	 * **********************************************************************/
	private String connectToServer(int val, String hashString, String keyVal, String message, DataOutputStream servOut, DataInputStream servIn) throws NumberFormatException, UnknownHostException, IOException, NoSuchAlgorithmException, InterruptedException {
		String res = null;

		if (val == 0){
			val = 8;
		}

		//Sending key-value pairs for PUT operation
		if(message.equalsIgnoreCase("PUT")){
			servOut.writeUTF(hashString+","+keyVal);
			res = servIn.readUTF();
		}
		//Sending key-value pairs for GET operation
		else if(message.equalsIgnoreCase("GET")){
			servOut.writeUTF(hashString);
			res = servIn.readUTF();
		}
		//Sending key-value pairs for DEL operation
		else if(message.equalsIgnoreCase("DEL")){
			servOut.writeUTF(hashString);
			res = servIn.readUTF();
		}
		return res;
	}

	/* **********************************************************************
	 * Method Name 	:	connectServers
	 * Parameters	:	String
	 * Returns		:	void
	 * Description	:	Method to creating connection with rest of the servers
	 * 					in the server cluster.
	 * **********************************************************************/
	private void connectServers(String key) throws NumberFormatException, UnknownHostException, IOException {

		ConfigReader prop = new ConfigReader();
		String[] name, temp;
		String[] server = new String[7];
		int counter = 0;
		Socket servTemp;
		DataOutputStream servOut;
		DataInputStream servIn;

		//Getting the address and port of all the servers from the config file
		for (int i = 1; i <= 8; i++){
			if(i != Integer.parseInt(key)){
				name = prop.read(String.valueOf(i));
				server[counter] = i + ":" + name[0] + ":" + name[1];
				counter++;
			}
		}

		//Creating socket connections with rest of server in the cluster
		for(int i = 0; i < 7; i++){
			temp = server[i].split(":");
			servTemp = new Socket(temp[1], Integer.parseInt(temp[2]));
			servOut = new DataOutputStream(servTemp.getOutputStream());
			servIn = new DataInputStream(servTemp.getInputStream());
			servers.put(Integer.parseInt(temp[0]), servOut);	//Adding Output streams to the hash table
			serversIn.put(Integer.parseInt(temp[0]), servIn);	//Adding input streams to the hash table
		}

		set = 1;
	}

	/* **********************************************************************
	 * Method Name 	:	mapper
	 * Parameters	:	int
	 * Returns		:	DataOutputStream
	 * Description	:	Method to retrieve output streams from the hash table.
	 * **********************************************************************/
	public DataOutputStream mapper(int val){
		if(val == 0){
			val = 8;
		}
		return servers.get(val);
	}

	/* **********************************************************************
	 * Method Name 	:	mapperIn
	 * Parameters	:	int
	 * Returns		:	DataInputStream
	 * Description	:	Method to retrieve input streams from the hash table.
	 * **********************************************************************/
	public DataInputStream mapperIn(int val){
		if(val == 0){
			val = 8;
		}
		return serversIn.get(val);
	}

}
