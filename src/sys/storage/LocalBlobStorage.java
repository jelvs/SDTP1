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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class LocalBlobStorage implements BlobStorage {
    private static final int BLOCK_SIZE = 512;

    String namenode;
    String[] datanodes;

    public LocalBlobStorage() {
        this.namenode = "http://localhost:9091";
        this.datanodes = new String[]{"http://localhost:9999"};
    }

    @Override
    public List<String> listBlobs(String prefix) {
        ClientConfig config = new ClientConfig();
        Client client = ClientBuilder.newClient(config);

        URI baseURI = UriBuilder.fromUri(namenode).build();
        WebTarget target = client.target(baseURI);

        Response response = target.path("/namenode/list/")
                .queryParam("prefix", prefix)
                .request()
                .get();

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
            
            Response response = target.path("/namenode/" + blob)
                    .request()
                    .get();

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

                URI baseURI2 = UriBuilder.fromUri(datanodes[0]).build();
                WebTarget target2 = client2.target(baseURI2);

                Response response2 = target2.path("/datanode/" + block)
                        .request()
                        .delete();
               
                if( response2.getStatusInfo().equals(Response.Status.NO_CONTENT) ) {
                    System.out.println( "deleted data resource...");
                } else
                    System.err.println( response2.getStatus() );

            });
        });

        ClientConfig config3 = new ClientConfig();
        Client client3 = ClientBuilder.newClient(config3);

        URI baseURI3 = UriBuilder.fromUri(namenode).build();
        WebTarget target3 = client3.target(baseURI3);

        Response response3 = target3.path("/namenode/list/")
                .queryParam("prefix", prefix)
                .request()
                .delete();
        if( response3.getStatusInfo().equals(Response.Status.NO_CONTENT) ) {
            System.out.println( "deleted data resource...");
        } else
            System.err.println( response3.getStatus() );

    }

    @Override
    public BlobReader readBlob(String name) {
        return new BufferedBlobReader(name, namenode, datanodes[0]);
    }

    @Override
    public BlobWriter blobWriter(String name) {
        return new BufferedBlobWriter(name, namenode, datanodes, BLOCK_SIZE);
    }
}
