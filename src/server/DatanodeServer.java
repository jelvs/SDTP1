package server;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import sys.storage.DatanodeResources;

import java.io.IOException;
import java.net.*;

public class DatanodeServer {

	private static URI baseURI;


	public static void main(String[] args) throws Exception {


		//create Server
		baseURI = URI.create(String.format("http://" + InetAddress.getLocalHost().getHostAddress() + ":8080/"));
		System.out.println(baseURI);
		ResourceConfig config = new ResourceConfig();
		config.register(new DatanodeResources());

		JdkHttpServerFactory.createHttpServer(baseURI, config);

		System.err.println("Server ready....");
		
		try {
			final int MAX_DATAGRAM_SIZE = 65536;
			final InetAddress group = InetAddress.getByName("238.69.69.69");
			if( ! group.isMulticastAddress()) {
				System.out.println( "Not a multicast address (use range : 224.0.0.0 -- 239.255.255.255)");
				System.exit(1);
			}

			MulticastSocket socket = new MulticastSocket( 9000 );
			socket.joinGroup(group);
			while( true ) {
				byte[] buffer = new byte[MAX_DATAGRAM_SIZE] ;
				DatagramPacket request = new DatagramPacket( buffer, buffer.length ) ;
				socket.receive( request );
				//System.out.write( request.getData(), 0, request.getLength() ) ;
				//prepare and send reply... (unicast)
				String requested = new String(request.getData());
				if(requested.equals("Datanode")) {
				System.out.println(requested);
				processMessage(socket, request);
				}

			}    
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	


	private static void processMessage(MulticastSocket socket, DatagramPacket request) throws IOException {
		//System.out.println("processMessage");
		System.out.println(new String(request.getData()));
		//System.out.println("x : " + new String(request.getData()));
		//String x = new String(request.getData()).trim();
		//if(x.equals("BlobStorage")) {
		String url = new String (request.getData());
		byte[] buffer = baseURI.toString().getBytes() ;

		DatagramPacket reply = new DatagramPacket(buffer, buffer.length, request.getAddress(), request.getPort());
		System.out.println("Namenode: " + "Address: " + request.getAddress() + "Port: " + request.getPort());
		socket.send(reply);


		//DatagramPacket reply = new DatagramPacket(buffer, buffer.length);
		//System.out.println("Datanode: " + "Address: " + request.getAddress() + "Port: " + request.getPort());
		//reply.setAddress(request.getAddress());
		//reply.setPort(request.getPort());
		//socket.send(reply);
		//}



	}











}
