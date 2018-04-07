package server;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import server.DatanodeServer.HeartBeat;
import sys.storage.DatanodeResources;
import sys.storage.NamenodeResources;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.net.URI;

public class NamenodeServer {


	private static URI baseURI;
	private static final String HEARTBEAT_MESSAGE = "ImAlive....";
	private static final int SOCKET_TIMEOUT = 1000;
	private static final int MAX_DATAGRAM_SIZE = 65536;
	private static final String MULTICAST_MESSAGE = "Namenode";

    private static final String MULTICAST_ADDRESS = "226.226.226.226";
	private static final int MULTICAST_PORT = 9000;

    public static void main(String[] args) throws Exception {


        //create Server
        String baseURI = String.format("http://"+ InetAddress.getLocalHost().getHostAddress() +":8081/");
        System.out.println(baseURI );
        ResourceConfig config = new ResourceConfig();
        config.register( new NamenodeResources() );

        JdkHttpServerFactory.createHttpServer( URI.create(baseURI), config);

        System.err.println("Server ready....");

      //multicast request sent up to 5 times
      		for (int retry = 0; retry < 5; retry++) {
      			try( MulticastSocket socket = new MulticastSocket()) {
      				
      					byte[] buffer = new byte[MAX_DATAGRAM_SIZE] ;
      					DatagramPacket request = new DatagramPacket( buffer, buffer.length ) ;
      					socket.setSoTimeout(SOCKET_TIMEOUT);
      					
      					multicastMessage(socket, MULTICAST_MESSAGE);
      					
      					
      					socket.receive( request );
      					System.out.write( request.getData(), 0, request.getLength() ) ;
      					break;
      					//prepare and send reply... (unicast)		
      				
      				
      			} catch (SocketTimeoutException e) {
      				System.out.println("Socket timeout!!!!!!");
      			} catch (IOException ex) {
      				//IO error
      			}
      		}
      		new Thread(new HeartBeat()).start();

      	}



      	private static void multicastMessage(MulticastSocket socket, String message ) throws IOException {
      		try {
      			
      			final InetAddress mAddress = InetAddress.getByName(MULTICAST_ADDRESS);
      			if (!mAddress.isMulticastAddress()) {
      				System.out.println("Use range : 224.0.0.0 -- 239.255.255.255");
      			}
      			
      			byte[] input = baseURI.toString().getBytes();
      			DatagramPacket reply = new DatagramPacket(input, input.length);

      			//set reply packet destination
      			reply.setAddress(mAddress);
      			reply.setPort(MULTICAST_PORT);
      			
      			socket.send(reply);
      		} catch (IOException ex) {
      			System.err.println("Error processing message from client. No reply was sent");
      		}
      	}

      	/**
      	 * Thread class that handles the heartbeat system
      	 */
      	static class HeartBeat implements Runnable {

      		public void run() {
      			while (true) {

      				try {
      					MulticastSocket socket = new MulticastSocket();


      					//Identifiyng the sender of the message
      					String message = HEARTBEAT_MESSAGE + "/" + InetAddress.getByName(MULTICAST_ADDRESS);

      					multicastMessage(socket, message);

      					Thread.sleep(3000);

      				} catch (IOException | InterruptedException ex) {
      					//Some error occured
      				}
      			}
      		}
      	}
      	
}