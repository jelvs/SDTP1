package server;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import sys.storage.NamenodeResources;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.URI;

public class NamenodeServer {


	private static URI baseURI;

	public static void main(String[] args) throws Exception {


		//create Server
		baseURI = URI.create(String.format("http://" + InetAddress.getLocalHost().getHostAddress() + ":8081/"));
		System.out.println(baseURI );
		ResourceConfig config = new ResourceConfig();
		config.register( new NamenodeResources() );

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
			//System.out.println(group);
			socket.joinGroup(group);
			while( true ) {
				byte[] buffer = new byte[MAX_DATAGRAM_SIZE] ;
				DatagramPacket request = new DatagramPacket( buffer, buffer.length ) ;
				socket.receive( request );
				System.out.println("fuck yeah : " + new String(request.getData()));
				//System.out.write( request.getData(), 0, request.getLength() ) ;
				//prepare and send reply... (unicast)
				processMessage(socket, request);

			}    
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}


	private static void processMessage(MulticastSocket socket, DatagramPacket request) throws IOException {
		//System.out.println("processMessage");
		//System.out.println("x : " + new String(request.getData()));
		//String x = new String(request.getData()).trim();
		//if(x.equals("BlobStorage")) {
			String url = new String (request.getData());
			byte[] buffer = "Namenode".getBytes() ;

			DatagramPacket reply = new DatagramPacket(buffer, buffer.length);
			System.out.println("Namenode: " + "Address: " + request.getAddress() + "Port: " + request.getPort());

			reply.setAddress(request.getAddress());
			reply.setPort(request.getPort());

			socket.send(reply);




		//}
	}


}