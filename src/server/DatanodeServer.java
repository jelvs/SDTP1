package server;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import sys.storage.DatanodeClient;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.URI;

public class DatanodeServer {

    private static URI baseURI;

    public static void main(String[] args) throws Exception {


        //create Server
        String baseURI = String.format("http://"+ InetAddress.getLocalHost().getHostAddress() +":9999/");
        System.out.println(baseURI );
        ResourceConfig config = new ResourceConfig();
        config.register( new DatanodeClient() );

        JdkHttpServerFactory.createHttpServer( URI.create(baseURI), config);

        System.err.println("Server ready....");






    }

    private static void multicastMessage(DatagramPacket packet, MulticastSocket socket) {
        try {
            byte[] input = baseURI.toString().getBytes();
            DatagramPacket reply = new DatagramPacket(input, input.length);

            //set reply packet destination
            reply.setAddress(packet.getAddress());
            reply.setPort(packet.getPort());

            socket.send(reply);
        } catch (IOException ex) {
            System.err.println("Error processing message from client. No reply was sent");
        }
    }








}
