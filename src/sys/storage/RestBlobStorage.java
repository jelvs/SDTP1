package sys.storage;

import api.storage.BlobStorage;
import client.DatanodeClient;
import client.NamenodeClient;
import sys.storage.io.BufferedBlobReader;
import sys.storage.io.BufferedBlobWriter;


import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RestBlobStorage implements BlobStorage {
	private static final int BLOCK_SIZE = 512;
	private static final int SOCKET_TIMEOUT = 10000;
	private static final int MAX_DATAGRAM_SIZE = 65536;
	private static final String NAMENODE_MESSAGE = "Namenode";
	private static final String DATANODE_MESSAGE = "Datanode";
	//private static final String MULTICAST_ADDRESS = "238.69.69.69";
	//private static final int MULTICAST_PORT = 9000;


	NamenodeClient namenode;
	ConcurrentHashMap<String, DatanodeClient> datanodes;


	public RestBlobStorage() {
		this.datanodes =  new ConcurrentHashMap<String, DatanodeClient>();
		runMulticast();
	}

	public void addDataNodeServer(String datanode) {	
		//System.out.println("blobStaddDatan : "+ datanode);
		datanodes.putIfAbsent(datanode, new DatanodeClient(datanode));
	}

	public void addNameNodeServer(String namenode) {
		//System.out.println("blobStaddNAME : "+ namenode);
		this.namenode = new NamenodeClient(namenode);
	}

	/*
	private void garbageCollection() {
		if(namenode.listAll() != null) {
			//Find all the datanodes
			for (String URL : namenode.listAll()) {

				//instanciate the map of blocks to keep
				Map<String, List<String>> blocksToKeep = new HashMap<>();
				blocksToKeep.put(URL, new ArrayList<String>());

				//getting the actualblocks for each datanode
				namenode.list( URL ).forEach( blob -> {
					namenode.read( blob ).forEach( block -> {
						blocksToKeep.get(URL).add(block);
					});
				});

				//update the datanode
				datanodes.get(URL).blocksToKeep(blocksToKeep.get(URL));;
			}
		}


	}
	*/
	@Override
	public List<String> listBlobs(String prefix) {
		return namenode.list(prefix);
	}

	@Override
	public void deleteBlobs(String prefix) {
		namenode.list( prefix ).forEach( blob -> {
			namenode.read( blob ).forEach( block -> {
				datanodes.get(prefix).deleteBlock(block);
			});
		});
		namenode.delete(prefix);
	}




	@Override
	public BlobReader readBlob(String name) {

		return new BufferedBlobReader( name, namenode, datanodes);		
	}

	@Override
	public BlobWriter blobWriter(String name) {
		DatanodeClient[] datanodess = datanodes.values().toArray(new DatanodeClient[datanodes.keySet().size()]);
		return new BufferedBlobWriter( name, namenode, datanodess , BLOCK_SIZE);
	}


	private void runMulticast() {
		/*new Thread(() -> {
			
			while(true) {
				garbageCollection();
				try {
					garbageCollection();
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
		*/
		while(datanodes.size() == 0 || namenode == null) {

			try( MulticastSocket socket = new MulticastSocket(9000)) {
				int counter = 0;

				byte[] buffer = new byte[MAX_DATAGRAM_SIZE] ;
				DatagramPacket request = new DatagramPacket( buffer, buffer.length ) ;
				//socket.setSoTimeout(SOCKET_TIMEOUT);
				String message;
				if(namenode == null) {
					message = NAMENODE_MESSAGE;
				}
				else {
					message = DATANODE_MESSAGE;
				}
				multicastMessage(socket, message);
				if(message == DATANODE_MESSAGE) {
					socket.setSoTimeout(SOCKET_TIMEOUT);

					while(counter != 10) {

						socket.receive( request );

						DiscoverData(request, message);

						System.out.write( request.getData(), 0, request.getLength() ) ;
						counter++;
					}
				}
				else {
					socket.setSoTimeout(SOCKET_TIMEOUT);

					socket.receive( request );

					DiscoverData(request, message);

					System.out.write( request.getData(), 0, request.getLength() ) ;}
				//prepare and send reply... (unicast)	



				//Thread.sleep(10000);
			} catch (SocketTimeoutException e) {
				System.out.println("Socket timeout!!!!!!");
				e.printStackTrace();
			} catch (IOException ex) {
				//IO error
				ex.printStackTrace();
			}
		}
		System.out.println("exit while\n");


		new Thread(() -> {
			while(true) {
				try( MulticastSocket socket = new MulticastSocket(9000)) {
					byte[] buffer = new byte[MAX_DATAGRAM_SIZE] ;
					DatagramPacket request = new DatagramPacket( buffer, buffer.length ) ;
					//socket.setSoTimeout(SOCKET_TIMEOUT);

					multicastMessage(socket, NAMENODE_MESSAGE);

					socket.receive( request );

					DiscoverData(request, NAMENODE_MESSAGE );

					System.out.write( request.getData(), 0 , request.getLength() ) ;

					//Thread.sleep(10000);
				} catch (SocketTimeoutException e) {
					e.printStackTrace();
				} catch (IOException ex) {
					//IO error
					ex.printStackTrace();
				}
			}
		}).start();
		new Thread(() -> {
			while(true) {
				try( MulticastSocket socket = new MulticastSocket(9000)) {
					byte[] buffer = new byte[MAX_DATAGRAM_SIZE] ;
					DatagramPacket request = new DatagramPacket( buffer, buffer.length ) ;
					//socket.setSoTimeout(SOCKET_TIMEOUT);
					multicastMessage(socket, DATANODE_MESSAGE);
					while(true) {
						socket.receive( request );
						DiscoverData(request, DATANODE_MESSAGE );
						System.out.write( request.getData(), 0 , request.getLength() ) ;
					}
					//Thread.sleep(10000);
				} catch (SocketTimeoutException e) {
					e.printStackTrace();
				} catch (IOException ex) {
					//IO error
					ex.printStackTrace();
				}
			}
		}).start();
	}

	/*
	 * Filters the messages from Datanode & Namenode
	 */
	private void DiscoverData(DatagramPacket request, String localMessage) {

		String message = new String(request.getData(), 0 , request.getLength());
		if(localMessage.equals(DATANODE_MESSAGE)){
			//System.out.println("url: :" + url);
			addDataNodeServer(message);
		}
		else if(localMessage.equals(NAMENODE_MESSAGE)) {
			addNameNodeServer(message);

		}
	}



	private static void multicastMessage(MulticastSocket socket, String sendmessage) throws IOException {

		try {

			byte[] input = sendmessage.getBytes();
			DatagramPacket reply = new DatagramPacket(input, input.length);
			//set reply packet destination
			final InetAddress group = InetAddress.getByName("238.69.69.69");
			if( ! group.isMulticastAddress()) {
				System.out.println( "Not a multicast address (use range : 224.0.0.0 -- 239.255.255.255)");
				System.exit(1);
			}
			reply.setAddress(group);
			reply.setPort(9000);

			socket.send(reply);
		} catch (IOException ex) {
			System.err.println("Error processing message from client. No reply was sent");
		}
	}
}
