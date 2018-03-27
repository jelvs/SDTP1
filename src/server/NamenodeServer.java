package server;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import sys.storage.DatanodeClient;
import sys.storage.NamenodeClient;

import java.net.InetAddress;
import java.net.URI;

public class NamenodeServer {

    private static URI baseURI;

    public static void main(String[] args) throws Exception {


        //create Server
        String baseURI = String.format("http://"+ InetAddress.getLocalHost().getHostAddress() +":9091/");
        System.out.println(baseURI );
        ResourceConfig config = new ResourceConfig();
        config.register( new NamenodeClient() );

        JdkHttpServerFactory.createHttpServer( URI.create(baseURI), config);

        System.err.println("Server ready....");






    }
}
