package sys.storage;

import api.storage.BlobStorage;
import client.DatanodeClient;
import client.NamenodeClient;
import org.glassfish.jersey.client.ClientConfig;
import sys.storage.io.BufferedBlobReader;
import sys.storage.io.BufferedBlobWriter;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import api.storage.BlobStorage;
import api.storage.Datanode;
import api.storage.Namenode;
import sys.storage.io.BufferedBlobReader;
import sys.storage.io.BufferedBlobWriter;

public class RestBlobStorage implements BlobStorage {
	private static final int BLOCK_SIZE = 512;
	private static URI baseURI ;
	private static final int SOCKET_TIMEOUT = 10000;
	private static final int MAX_DATAGRAM_SIZE = 65536;
	private static final String HEARTBEAT_MESSAGE = "ImAlive....";
	private static final String MULTICAST_MESSAGE = "BlobStorage";
	private static final String NAMENODE_MESSAGE = "Namenode";
	private static final String DATANODE_MESSAGE = "Datanode";
	private static final String MULTICAST_ADDRESS = "238.69.69.69";
	private static final int MULTICAST_PORT = 9000;


	NamenodeClient namenode;
	ConcurrentHashMap<String, DatanodeClient> datanodes;


	public RestBlobStorage() {
		this.datanodes =  new ConcurrentHashMap<String, DatanodeClient>();
		runMulticast();
	}

	public void addDataNodeServer(String datanode) {	
		datanodes.putIfAbsent(datanode, new DatanodeClient(datanode));
	}

	public void addNameNodeServer(String namenode) {
		this.namenode = new NamenodeClient(namenode);
	}

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

		return new BufferedBlobWriter( name, namenode, datanodes, BLOCK_SIZE);
	}


	private void runMulticast() {

		final int NUM_TRIES = 5;

		for (int i = 0; i < NUM_TRIES; i++) {

			while(datanodes.size() == 0 || namenode == null) {
				try( MulticastSocket socket = new MulticastSocket(9000)) {


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

					socket.receive( request );

					DiscoverData(request, message);

					System.out.write( request.getData(), 0, request.getLength() ) ;
					//prepare and send reply... (unicast)	


				} catch (SocketTimeoutException e) {
					System.out.println("Socket timeout!!!!!!");
				} catch (IOException ex) {
					//IO error
					ex.printStackTrace();
				}
			}
			System.out.println("exit while\n");
		}

			new Thread(() -> {
				while(true) {
					try( MulticastSocket socket = new MulticastSocket(9000)) {

						byte[] buffer = new byte[MAX_DATAGRAM_SIZE] ;
						DatagramPacket request = new DatagramPacket( buffer, buffer.length ) ;
						socket.setSoTimeout(SOCKET_TIMEOUT);

						multicastMessage(socket, DATANODE_MESSAGE);

						socket.receive( request );

						DiscoverData(request, DATANODE_MESSAGE );

						System.out.write( request.getData(), 0, request.getLength() ) ;
						//prepare and send reply... (unicast)	

						//Thread.sleep(10000);




					} catch (SocketTimeoutException e) {
						System.out.println("Socket timeout!!!!!!");
					} catch (IOException ex) {
						//IO error
						ex.printStackTrace();
					}
				}


			}).start();
		

		//new Thread(new HeartBeat()).start();
	}


	/*
	 * Filters the messages from Datanode & Namenode
	 */
	private void DiscoverData(DatagramPacket request, String localMessage) {
		//System.out.println("before if : " + request.getAddress());
		//System.out.println("Message : " + new String(request.getData()));
		String message = new String(request.getData());

		if(localMessage.equals(DATANODE_MESSAGE)){
			String url = String.format(message);
			System.out.println("url : " + url);


			addDataNodeServer(url);
		}

		else if(localMessage.equals(NAMENODE_MESSAGE)) {

			String url = String.format(message);
			System.out.println("url : " + url);


			addNameNodeServer(url);
		}else {
			System.out.println("inside else....\n");
		}
	}



	private static void multicastMessage(MulticastSocket socket, String sendmessage) throws IOException {

		try {

			byte[] input = sendmessage.getBytes();
			DatagramPacket reply = new DatagramPacket(input, input.length);

			//set reply packet destination
			final InetAddress mAddress = InetAddress.getByName(MULTICAST_ADDRESS);
			reply.setAddress(mAddress);
			reply.setPort(MULTICAST_PORT);

			socket.send(reply);
		} catch (IOException ex) {
			System.err.println("Error processing message from client. No reply was sent");
		}
	}
}

