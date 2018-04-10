package server;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import sys.storage.DatanodeResources;

import java.io.IOException;
import java.net.*;

import javax.ws.rs.ProcessingException;

public class DatanodeServer {

	public static URI baseURI;


	public static void main(String[] args) throws Exception {
		System.setProperty("java.net.preferIPv4Stack", "true");

		//create Server
		baseURI = URI.create(String.format("http://" + InetAddress.getLocalHost().getHostAddress() + ":8080/"));
		System.out.println(baseURI + ": ola");
		ResourceConfig config = new ResourceConfig();
		config.register(new DatanodeResources(baseURI));

		JdkHttpServerFactory.createHttpServer(baseURI, config);

		System.err.println("Server ready....");

		//final int NUM_TRIES = 5, SLEEP_TIME = 1000;
		
		//for (int i = 0; i < NUM_TRIES; i++) {
			try {
				final int MAX_DATAGRAM_SIZE = 65536;
				final InetAddress group = InetAddress.getByName("238.69.69.69");
				if( ! group.isMulticastAddress()) {
					System.out.println( "Not a multicast address (use range : 224.0.0.0 -- 239.255.255.255)");
					System.exit(1);
				}

				MulticastSocket socket = new MulticastSocket( 9000 );
				socket.joinGroup(group);
				int counter = 0;
				while(counter == 0) {	
					byte[] buffer = new byte[MAX_DATAGRAM_SIZE] ;
					DatagramPacket request = new DatagramPacket( buffer, buffer.length ) ;
					socket.receive( request );
					String requested = new String(request.getData(), 0,request.getLength());
					System.out.println("Datanode Recieved : " + new String(request.getData()));
					//System.out.write( request.getData), 0, request.getLength() ) ;
					//prepare and send reply... (unicast)
					if(requested.contains("Datanode")) {
						System.out.println(requested + "passou");
						processMessage(socket, request);
						counter++;
						Thread.sleep(10000);
					}
				}
				   
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		
	}




	private static void processMessage(MulticastSocket socket, DatagramPacket request) throws IOException {

		//String url = new String (request.getData());
		System.out.println(baseURI.toString());
		byte[] buffer = baseURI.toString().getBytes() ;
		//byte[] buffer = badjoraz.getBytes();
		DatagramPacket reply = new DatagramPacket(buffer, buffer.length, request.getAddress(), request.getPort());
		System.out.println("Datanode: " + "Address: " + request.getAddress() + "Port: " + request.getPort());
		socket.send(reply);




		//}
	}


}












