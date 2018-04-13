package server;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import sys.storage.NamenodeResources;
import utils.IP;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.URI;

import javax.ws.rs.ProcessingException;

public class NamenodeServer {


	private static URI baseURI;
	private static final int NUM_TRIES = 5;

	static String badjoraz;
	public static void main(String[] args) throws Exception {
		System.setProperty("java.net.preferIPv4Stack", "true");

		//create Server
		baseURI = URI.create(String.format("http://" + IP.hostAddress() + ":8081/"));
		System.out.println(baseURI );
		ResourceConfig config = new ResourceConfig();
		config.register( new NamenodeResources() );

		JdkHttpServerFactory.createHttpServer(baseURI, config);

		System.err.println("Server ready....");


		for(int i = 0; i <NUM_TRIES; i++) {
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
				while(counter==0) {
					byte[] buffer = new byte[MAX_DATAGRAM_SIZE] ;
					DatagramPacket request = new DatagramPacket( buffer, buffer.length ) ;
					socket.receive( request );
					String requested = new String(request.getData(), 0,request.getLength());

					System.out.write(request.getData(), 0, request.getLength()) ;
					//prepare and send reply... (unicast)
					if(requested.contains("Namenode")) {
						//System.out.println(requested + "passou");
						processMessage(socket, request);
						counter ++;
					}

				}  

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}new Thread(() -> {
			HeartBeat();

		}).start();

	}

	private static void HeartBeat() {
		try {
			final int MAX_DATAGRAM_SIZE = 65536;
			final InetAddress group = InetAddress.getByName("238.69.69.69");
			if( ! group.isMulticastAddress()) {
				System.out.println( "Not a multicast address (use range : 224.0.0.0 -- 239.255.255.255)");
				System.exit(1);
			}
			MulticastSocket socket = new MulticastSocket( 9000 );
			socket.joinGroup(group);
			while(true) {
				byte[] buffer = new byte[MAX_DATAGRAM_SIZE] ;
				DatagramPacket request = new DatagramPacket( buffer, buffer.length ) ;
				socket.receive( request );
				String requested = new String(request.getData(), 0,request.getLength());

				System.out.write(request.getData(), 0, request.getLength()) ;

				//prepare and send reply... (unicast)
				if(requested.contains("Namenode")) {
					processMessage(socket, request);
				}  
				Thread.sleep(1000);
			}
		}catch (IOException e) {
			e.printStackTrace();
		}catch (InterruptedException e) {
			e.printStackTrace();
		}

	}



	private static void processMessage(MulticastSocket socket, DatagramPacket request) throws IOException {

		byte[] buffer = baseURI.toString().getBytes() ;
		DatagramPacket reply = new DatagramPacket(buffer, buffer.length, request.getAddress(), request.getPort());
		socket.send(reply);





	}


}