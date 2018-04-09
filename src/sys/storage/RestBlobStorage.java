package sys.storage;

import api.storage.BlobStorage;

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



	static String namenode;
	ArrayList<String> datanodes;

	public RestBlobStorage() {
		this.namenode = null;
		this.datanodes = new ArrayList<String>();
		runMulticast();
	}

	public void addDataNodeServer(String datanode) {
		datanodes.add(datanode);
	}

	public void addNameNodeServer(String namenode) {
		this.namenode = namenode;
	}

	@Override
	public List<String> listBlobs(String prefix) {
		ClientConfig config = new ClientConfig();
		Client client = ClientBuilder.newClient(config);

		URI baseURI = UriBuilder.fromUri(namenode).build();
		WebTarget target = client.target(baseURI);

		Response response = target.path("/namenode/list/").queryParam("prefix", prefix).request().get();

		if (response.hasEntity()) {
			List<String> data = response.readEntity(List.class);
			return data;
		} else
			System.err.println(response.getStatus());
		return new ArrayList<String>();

	}

	@SuppressWarnings("unchecked")
	@Override
	public void deleteBlobs(String prefix) {
		List<String> blobs = listBlobs(prefix);
		blobs.forEach(blob -> {

			ClientConfig config = new ClientConfig();
			Client client = ClientBuilder.newClient(config);

			URI baseURI = UriBuilder.fromUri(namenode).build();
			WebTarget target = client.target(baseURI);

			Response response = target.path("/namenode/" + blob).request().get();

			List<String> blocks;

			if (response.hasEntity()) {
				blocks = response.readEntity(List.class);
			} else {
				System.err.println(response.getStatus());
				blocks = new ArrayList<String>();
			}

			blocks.forEach(block -> {

				ClientConfig config2 = new ClientConfig();
				Client client2 = ClientBuilder.newClient(config2);

				URI baseURI2 = UriBuilder.fromUri(datanodes.get(0)).build();
				WebTarget target2 = client2.target(baseURI2);

				Response response2 = target2.path("/datanode/" + block).request().delete();

				if (response2.getStatusInfo().equals(Response.Status.NO_CONTENT)) {
					System.out.println("deleted data resource...");
				} else
					System.err.println(response2.getStatus());

			});
		});

		ClientConfig config3 = new ClientConfig();
		Client client3 = ClientBuilder.newClient(config3);

		URI baseURI3 = UriBuilder.fromUri(namenode).build();
		WebTarget target3 = client3.target(baseURI3);

		Response response3 = target3.path("/namenode/list/").queryParam("prefix", prefix).request().delete();
		if (response3.getStatusInfo().equals(Response.Status.NO_CONTENT)) {
			System.out.println("deleted data resource...");
		} else
			System.err.println(response3.getStatus());

	}

	@Override
	public BlobReader readBlob(String name) {
		return new BufferedBlobReader(name, namenode, datanodes.get(0));
	}

	@Override
	public BlobWriter blobWriter(String name) {
		return new BufferedBlobWriter(name, namenode, datanodes, BLOCK_SIZE);
	}
	private void runMulticast() {
		while(datanodes.isEmpty() || namenode == null) {
			try( MulticastSocket socket = new MulticastSocket(9000)) {


				byte[] buffer = new byte[MAX_DATAGRAM_SIZE] ;
				DatagramPacket request = new DatagramPacket( buffer, buffer.length ) ;
				socket.setSoTimeout(SOCKET_TIMEOUT);

				multicastMessage(socket, MULTICAST_MESSAGE);

				socket.receive( request );

				DiscoverData( request );

				System.out.write( request.getData(), 0, request.getLength() ) ;
				//prepare and send reply... (unicast)	




				System.out.println("exit while\n");
			} catch (SocketTimeoutException e) {
				System.out.println("Socket timeout!!!!!!");
			} catch (IOException ex) {
				//IO error
				ex.printStackTrace();
			}
		}


		//new Thread(new HeartBeat()).start();
	}


	/*
	 * Filters the messages from Datanode & Namenode
	 */
	private void DiscoverData(DatagramPacket request) {
		System.out.println("before if : " + request.getAddress());
		System.out.println("Message : " + new String(request.getData()));
		String message = new String(request.getData()).trim();

		if(message.equals(DATANODE_MESSAGE)){

			String url = String.format("http:/%s:%d", request.getAddress(), request.getPort());
			System.out.println("url : " + url);
			addDataNodeServer(new String (request.getData()));

		}else if(message.equals(NAMENODE_MESSAGE)) {

			String url = String.format("http:/%s:%d", request.getAddress(), request.getPort());
			System.out.println("url : " + url);
			addNameNodeServer(new String (request.getData()));
		}else {
			System.out.println("inside else....\n");
		}
	}



	private static void multicastMessage(MulticastSocket socket, String message ) throws IOException {
		String sendmessage;
		try {
			if(namenode == null) {
				sendmessage = NAMENODE_MESSAGE;
			}
			else {
				sendmessage = DATANODE_MESSAGE;
			}

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

