
/*
* Java Space servers run in the master-slave model. The 
* server administrator starts the master JSpace server that launches a slave server at each of remote 
* computing nodes, using the JSCH (secured ssh connection) library. Each server including the master creates a local hash table to maintain user data items, (each packetized in 
* an Entry object).
* @author sindhuri Bolisetty
* @version 2.5.2014
*/

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

public class JSpace {

	private final int READ = 0;
	private final int WRITE = 1;
	private final int TAKE = 2;

	DatagramSocket datagramSocket = null;
	DatagramPacket datagramPacket = null;
	MulticastSocket multicastSocket = null;
	BufferedReader commandLine = null;

	InetAddress group = null;
	
	int numServers;
	int serverID;
	byte buffer[];
	int port;
	Entry entry;
	boolean myturn[] = new boolean[1];      // space thread synchronization lock
	byte myHashTables[] = new byte[4];      //

	private final int NUMBER_OF_HASHTABLES = 4;     //number of hashtables.

	Hashtable<String, Entry> atof; // Hashtable for holding values of variable names from a to f
	Hashtable<String, Entry> gtol; // Hashtable for holding values of variable names from g to l
	Hashtable<String, Entry> mtos; // Hashtable for holding values of variable names from m to s
	Hashtable<String, Entry> ttoz; // Hashtable for holding values of variable names from t to z

	 //JSpace threads watches a multi-cast UDP message from a client
		private class SpaceThread extends Thread {
	
			public SpaceThread() {
				try {
					
					multicastSocket = new MulticastSocket(port);
					multicastSocket.joinGroup(group);
				} catch (IOException e) {
					e.printStackTrace();
				}
	
				//allocates one hash table to each server.
				int hashTableNumber = serverID % 4;
				myHashTables[hashTableNumber] = 1;
			}
	
			@Override
			public void run() {
	
				while (true) {
					buffer = new byte[1024];
					datagramPacket = new DatagramPacket(buffer, buffer.length);
	
					synchronized (myturn) {
	
						try {
							multicastSocket.receive(datagramPacket);
							InetAddress inetgroup = datagramPacket.getAddress();
							entry = Entry.deserialize(buffer);
	
							if (entry != null) {
								String myLockObject = entry.getType() + " " + entry.getName();
								SessionThread sessionthread = new SessionThread(entry, inetgroup, myLockObject);
								sessionthread.start();
	
							}
						} catch (IOException e) {
							e.printStackTrace();
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
					}
				}
	
			}
		}
	
		private class SessionThread extends Thread {
			Entry receivedEntry;
			InetAddress clientAddress;
			String lockObj;
	
			public SessionThread(Entry rec_Entry, InetAddress clientAddr, String myLockObject) {
				this.receivedEntry = rec_Entry;
				this.clientAddress = clientAddr;
				this.lockObj = myLockObject;
			}
	
			@Override
			public void run() {
				
				if (receivedEntry.getOperation() == READ) {
					Entry entryRead;
					// implement two conditions if entry present perform read else
					// suspend the thread until the required entry is present.
					synchronized (lockObj) {
						
						while(true)
						{
						    entryRead = readFromJSpace(receivedEntry);
	  					   if (entryRead == null) {
							try {
								lockObj.wait();
							} catch (InterruptedException e) {
								e.printStackTrace();
								// exit -1
							}
	  					   }
							else
								break;
						}
						
						buffer = new byte[1024];
						try {
							datagramSocket = new DatagramSocket();
							buffer = entry.serialize(entryRead);
							datagramPacket = new DatagramPacket(buffer, buffer.length,clientAddress,port);
							datagramSocket.send(datagramPacket);
						} catch (IOException e) {
						
							e.printStackTrace();
						}
						
					}
	
				} else if (receivedEntry.getOperation() == WRITE) {
					synchronized (lockObj) {
						
						writetoJSpace(receivedEntry);
						
						lockObj.notifyAll();
						
					}
	
				} else if (receivedEntry.getOperation() == TAKE) {
					Entry returnentry;
					
						returnentry = takeFromJSpace(receivedEntry);
						buffer = new byte[1024];
						try {
							datagramSocket = new DatagramSocket();
							buffer = entry.serialize(returnentry);
							datagramPacket = new DatagramPacket(buffer, buffer.length,clientAddress,port);
							datagramSocket.send(datagramPacket);
						} catch (IOException e) {
							
							e.printStackTrace();
						}
					}
						
				
			}
	
		}

		// salve body
	public JSpace(Connection connection) {

		try {
			String multicastgroup = (String) connection.in.readObject();
			port = (Integer) connection.in.readObject();
			numServers = (Integer) connection.in.readObject();
			serverID = (Integer) connection.in.readObject();
			System.out.println("Salve - " + serverID);

		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		}
		
		// TODO start a space thread.

		// start a space thread.
//			SpaceThread st = new SpaceThread();
//			st.start();

		
				while (true) {
					try {
						String input = (String) connection.in.readObject();
						if (input.equalsIgnoreCase("show")) {
							connection.out.writeObject(atof);
							connection.out.writeObject(gtol);
							connection.out.writeObject(mtos);
							connection.out.writeObject(ttoz);
						} else if (input.equalsIgnoreCase("quit")) {
							connection.out.writeObject("ACKNOWLEDGED");
							connection.out.flush();
							// TODO close all the threads.
						}
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
				}

	}

	// master body
	public JSpace(Connection connections[], String multicast_group, int port) {
		numServers = connections.length + 1; //total servers
		serverID = 0;    //serverid for master

		try {
			group = InetAddress.getByName(multicast_group);
			this.port = port;
		} catch (Exception e) {
			
		}
		try{
			for (int i = 0; i < connections.length; i++) {
				int serverId = i + 1;

				connections[i].out.writeObject(multicast_group);
				connections[i].out.writeObject(this.port);
				connections[i].out.writeObject(numServers);
				connections[i].out.writeObject(serverId);
				connections[i].out.flush();
				System.out.println(connections[i].in.readObject());
                
				// open log,write log,close log

			}
		}catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException cne) {
			//

		}
		
		// TODO start a space thread.
//		SpaceThread st = new SpaceThread();
//		st.start();

					commandLine = new BufferedReader(new InputStreamReader(System.in));
		
					try {
						while (true) {
							String input = commandLine.readLine();
		
							// write command line to the log.
							if (input.equalsIgnoreCase("show")) {
		
								// print all masters hash tables
								showHashTable(atof);
								showHashTable(gtol);
								showHashTable(mtos);
								showHashTable(ttoz);
		
								// ask the salve server to send their hash tables.
								for (int i = 0; i < connections.length; i++) {
									connections[i].out.writeObject("show");
									connections[i].out.flush();
		
//									for (int k = 0; k < NUMBER_OF_HASHTABLES; k++) {
//										Hashtable a = (Hashtable) connections[i].in.readObject();
//										Hashtable tempGtoL = (Hashtable) connections[i].in.readObject();
//										Hashtable tempMtoS = (Hashtable) connections[i].in.readObject();
//										Hashtable tempTtoZ = (Hashtable) connections[i].in.readObject();
//										showHashTable(tempAtoF); // write log
//										showHashTable(tempGtoL);
//										showHashTable(tempMtoS);
//										showHashTable(tempTtoZ);
//									}
		
								}
							} else if (input.equalsIgnoreCase("quit")) {
		
								for (int j = 0; j < connections.length; j++) {
									connections[j].out.writeObject("quit");
									Object acknowledgement = connections[j].in.readObject();
									// send quit command to salve.
									// receive ack from salve servers.
									// close all the threads.
								}
							} else {
								System.out.println("Invalid command ");
							}
		
						}
					} catch (Exception e) {
						
					}



	}

	// prints out all the values in the hash table on the master server.
		private void showHashTable(Hashtable hashtable) {
	
			Enumeration elements = hashtable.keys();
			while (elements.hasMoreElements()) {
				Entry e = (Entry) elements.nextElement();
				System.out.println(e.getName() + "," + e.getType() + "," + e.getValue()); // write a log
	
			}
	
		}
	
		// writing an entry into the JSpace
		private void writetoJSpace(Entry entry) {
			char startLetter = entry.getName().charAt(0);
			if (startLetter >= 'a' && startLetter <= 'f' && (myHashTables[0] == 1)) {
				atof.put(entry.getName(), entry);
			} else if (startLetter >= 'g' && startLetter <= 'l' && (myHashTables[1] == 1)) {
				gtol.put(entry.getName(), entry);
			} else if (startLetter >= 'm' && startLetter <= 's' && (myHashTables[2] == 1)) {
				mtos.put(entry.getName(), entry);
			} else {
				ttoz.put(entry.getName(), entry);
			}
	
		}
	
		// reading an entry from JSpace Servers.
		private Entry readFromJSpace(Entry entry) {
			char startLetter = entry.getName().charAt(0);
			if (startLetter >= 'a' && startLetter <= 'f' && (myHashTables[0] == 1)) {
				return atof.get(entry.getName());
			} else if (startLetter >= 'g' && startLetter <= 'l' && (myHashTables[1] == 1)) {
				return gtol.get(entry.getName());
			} else if (startLetter >= 'm' && startLetter <= 's' && (myHashTables[2] == 1)) {
				return mtos.get(entry.getName());
			} else {
				return ttoz.get(entry.getName());
	
			}
		}
	
		// removing an entry from the JSpace servers.
		private Entry takeFromJSpace(Entry entry) {
			char startLetter = entry.getName().charAt(0);
			if (startLetter >= 'a' && startLetter <= 'f' && (myHashTables[0] == 1)) {
				return atof.remove(entry.getName());
			} else if (startLetter >= 'g' && startLetter <= 'l' && (myHashTables[1] == 1)) {
				return gtol.remove(entry.getName());
			} else if (startLetter >= 'm' && startLetter <= 's' && (myHashTables[2] == 1)) {
				return mtos.remove(entry.getName());
			} else {
				return ttoz.remove(entry.getName());
			}
		}
}
