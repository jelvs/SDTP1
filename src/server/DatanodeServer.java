package server;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import sys.storage.DatanodeClient;

import java.io.IOException;
import java.net.*;

public class DatanodeServer {

    private static URI baseURI;
    private static final String HEARTBEAT_MESSAGE = "ImHere";
    private static final int SOCKET_TIMEOUT = 1000;

    //MultiCast
    private static final String MULTICAST_ADDRESS = "230.100.100.100";
    private static final int MULTICAST_PORT = 7070;

    public static void main(String[] args) throws Exception {


        //create Server
        String baseURI = String.format("http://" + InetAddress.getLocalHost().getHostAddress() + ":9999/");
        System.out.println(baseURI);
        ResourceConfig config = new ResourceConfig();
        config.register(new DatanodeClient());

        JdkHttpServerFactory.createHttpServer(URI.create(baseURI), config);

        System.err.println("Server ready....");

        MulticastSocket socket = new MulticastSocket();

        //Send multicast request with MESSAGE - Send up to three times
        for (int retry = 0; retry < 3; retry++) {

            try {

                byte[] buffer = new byte[65536];
                DatagramPacket url_packet = new DatagramPacket(buffer, buffer.length);
                socket.setSoTimeout(SOCKET_TIMEOUT);

                socket.receive(url_packet);

                /*


                Incompleto
                falta completar

                 */

            } catch (SocketTimeoutException e) {
                //No server responded within given time
            } catch (IOException ex) {
                //IO error
            }
        }

        //Creating keepAlive thread
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
