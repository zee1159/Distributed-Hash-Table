/***************************************************************************************\
 * Name			 : Zeeshan Aamir Khavas
 * Application	 : Distributed Hash Table P2P Application
 * Program		 : Client.java
 * Description   : This class is a runnable thread. I is instantiated for each client.
 * 				   It handles all the client by calling the Request class
 * Date			 : 10/13/2015
 * @author Zee
\***************************************************************************************/

package com.zee.dht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Client {
	static int key = 1, start, end, size;

	static String message;

	public static void main(String[] args) {
		String serverName;
		int port, chk = 1;
		Socket myClient;
		DataInputStream clientIn;
		DataOutputStream clientOut;

		Scanner scan = new Scanner(System.in);

		/*User is prompted for server address and details*/
		System.out.println("+--------------------------------+");
		System.out.println("|** Welcome to P2P(DHT) Client **|");
		System.out.println("+--------------------------------+");
		System.out.println("Enter Server address:");
		serverName = scan.nextLine();
		System.out.println("Enter Server port:");
		port = Integer.parseInt(scan.nextLine());

		try {
			myClient = new Socket(serverName, port);	//connection is made to the requested server
			System.out.println("\n---@ Connection to Server successful ! @---");

			//Taking range and size from user
			while(chk != 0){
				System.out.println("\nEnter size of DHT operations:");
				size = scan.nextInt();
				System.out.println("Enter range of keys (start): ");
				start = scan.nextInt();
				System.out.println("Enter range of keys (end): ");
				end = scan.nextInt();
				if(end - start != size){
					System.out.println("ERROR: Range of values should be equal to size !");
					chk = 1;
				}
				else
					chk = 0;
			}

			//Creating message of size 1004 Bytes
			StringBuffer outputBuffer = new StringBuffer(1004);
			for (int i = 0; i < 10004; i++){
				   outputBuffer.append("z");
				}
			message = outputBuffer.toString();

			//Instantiating IO stream objects for the client
			clientIn = new DataInputStream(myClient.getInputStream());
			clientOut = new DataOutputStream(myClient.getOutputStream());

			//display() will show user a application menu to user
			while(key != 4){
				displayMenu(myClient, clientOut, clientIn);
			}
			clientOut.writeUTF("CLOSE");		//if client wishes to close connection send server close request
			System.out.println("\n*   *   *   *   *   *   *   *   *   *   *   *   *   *   *");
			System.out.println("Thank you for using P2P client. Your connection is closed !");

		}
		catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			System.out.println("Connection to Server failed ! ");
			//e.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Connection to Server failed ! Server not available at requested address. ");
			//e.printStackTrace();
		}
		scan.close();
	}

	/* **********************************************************************
	 * Method Name 	:	displayMenu
	 * Parameters	:	Socket, DataOutputStream, DataInputStream
	 * Returns		:	void
	 * Description	:	This method will display a application menu to the user.
	 * 					It takes user input and calls respective method to handle
	 * 					requests.
	 * **********************************************************************/
	private static void displayMenu(Socket myClient, DataOutputStream clientOut, DataInputStream clientIn) throws IOException {
		Scanner scan = new Scanner(System.in);

		System.out.println("\n+-------------------------+");
		System.out.println("|      P2P(DHT) Menu      |");
		System.out.println("+-------------------------+");
		System.out.println("|    1. PUT operation     |");
		System.out.println("|    2. GET operation     |");
		System.out.println("|    3. DEL operation     |");
		System.out.println("|    4. Exit Network      |");
		System.out.println("+-------------------------+");
		System.out.println("Enter the selection number:");
		key = scan.nextInt();
		clientOut.writeUTF("CLIENT");

		/*based on user selection call respective methods*/
		if(key == 1){
			putOperation(myClient, clientOut, clientIn);	//Method for PUT operation
		}
		else if(key == 2){
			getOperation(myClient, clientOut, clientIn);	//Method for GET operation
		}
		else if(key == 3){
			delOperation(myClient, clientOut, clientIn);	//Method for DEL operation
		}
	}

	/* **********************************************************************
	 * Method Name 	:	delOperation
	 * Parameters	:	Socket, DataOutputStream, DataInputStream
	 * Returns		:	void
	 * Description	:	This method will send the keys to be deleted from the
	 * 					distributed server cluster and accepts acknowledgement
	 * 					for the same.
	 * **********************************************************************/
	private static void delOperation(Socket myClient, DataOutputStream clientOut, DataInputStream clientIn) throws IOException {
		String result;
		int chk = 0;

		clientOut.writeUTF("DEL");
		clientOut.writeInt(size);

		long lStartTime = System.currentTimeMillis();		//Starting timer to monitor the operation
		System.out.println("DEL Operation initiated....");

		for(int i = start; i < end; i++){
			clientOut.writeUTF(String.valueOf(i));		//Sending key to be deleted
			result = clientIn.readUTF();
			if(result.equalsIgnoreCase("FALSE")){
				System.out.println("DEL Operation failed for key: " + i);
				chk = 1;
			}
		}
		if(chk == 1)
			System.out.println("DEL Operation was not successsful !");
		else
			System.out.println("DEL Operation was successful !");
		long lEndTime = System.currentTimeMillis();			//Stopping timer
		long difference = lEndTime - lStartTime;
		System.out.println("Elapsed time for DEL: " + TimeUnit.MILLISECONDS.toSeconds(difference) +"secs");

	}

	/* **********************************************************************
	 * Method Name 	:	getOperation
	 * Parameters	:	Socket, DataOutputStream, DataInputStream
	 * Returns		:	void
	 * Description	:	This method will send the key for which the key-value
	 * 			 		to be stored on the distributed server cluster and
	 * 					accepts acknowledgement for the same.
	 * **********************************************************************/
	private static void getOperation(Socket myClient, DataOutputStream clientOut, DataInputStream clientIn) throws IOException {
		String keyVal = null;

		clientOut.writeUTF("GET");
		clientOut.writeInt(100000);
		System.out.println("GET Operation initiated....");
		long lStartTime = System.currentTimeMillis();		//Starting timer to monitor the operation

		for(int i = start; i < end; i++){
			clientOut.writeUTF(String.valueOf(i));			//sending key for which value will be retrieved
			keyVal = clientIn.readUTF();
		}

		long lEndTime = System.currentTimeMillis();			//Stopping timer
		long difference = lEndTime - lStartTime;
		System.out.println("Elapsed time for GET: " + TimeUnit.MILLISECONDS.toSeconds(difference) +"secs");
	}

	/* **********************************************************************
	 * Method Name 	:	putOperation
	 * Parameters	:	Socket, DataOutputStream, DataInputStream
	 * Returns		:	void
	 * Description	:	This method will send the key-value pair to be stored
	 * 				 	on the distributed server cluster and accepts acknowledgement
	 * 					for the same.
	 * **********************************************************************/
	private static void putOperation(Socket myClient, DataOutputStream clientOut, DataInputStream clientIn) throws IOException {
		String result;
		clientOut.writeUTF("PUT");
		clientOut.writeInt(100000);
		int chk = 0;
		System.out.println("PUT Operation initiated....");
		long lStartTime = System.currentTimeMillis();		//Starting timer to monitor the operation

		for(int i = start; i < end; i++){
			clientOut.writeUTF(i+":" +message);		//sending key-value pair to be stored on cluster
			result = clientIn.readUTF();
			if(result.equalsIgnoreCase("FALSE")){
				System.out.println("PUT Operation failed for key: " + i);
				chk = 1;
			}
		}
		if(chk == 1)
			System.out.println("PUT Operation was not successsful !");
		else
			System.out.println("PUT Operation was successful !");
		long lEndTime = System.currentTimeMillis();			//Stopping timer
		long difference = lEndTime - lStartTime;
		System.out.println("Elapsed time for PUT: " + TimeUnit.MILLISECONDS.toSeconds(difference) +"secs");
	}

}
