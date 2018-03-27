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

    @Override
    public void deleteBlobs(String prefix) {
        namenode.list(prefix).forEach(blob -> {
            namenode.read(blob).forEach(block -> {
                datanodes[0].deleteBlock(block);
            });
        });
        namenode.delete(prefix);
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
