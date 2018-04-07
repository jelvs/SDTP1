package sys.storage.io;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import org.glassfish.jersey.client.ClientConfig;

import api.storage.BlobStorage.BlobReader;
import api.storage.Datanode;
import api.storage.Namenode;

/*
 *
 * Implements BlobReader.
 *
 * Allows reading or iterating the lines of Blob one at a time, by fetching each block on demand.
 *
 * Intended to allow streaming the lines of a large blob (with mamy (large) blocks) without reading it all first into memory.
 */
public class BufferedBlobReader implements BlobReader {

    final String name;
    final String namenode;
    final String datanode;

    final Iterator<String> blocks;

    final LazyBlockReader lazyBlockIterator;

    public BufferedBlobReader(String name, String namenode, String datanode) {
        this.name = name;

        //HARDCoded para testar e ver se está a funcionar

        this.namenode = "http://localhost:9091";
        this.datanode = "http://localhost:9999";

        ClientConfig config = new ClientConfig();
        Client client = ClientBuilder.newClient(config);

        URI baseURI = UriBuilder.fromUri(namenode).build();
        WebTarget target = client.target(baseURI);
        @SuppressWarnings("unchecked")
		List<String> response = target.path("/namenode/" + name)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get(List.class);
       
        this.blocks = response.iterator();
        this.lazyBlockIterator = new LazyBlockReader();
    }

    @Override
    public String readLine() {
        return lazyBlockIterator.hasNext() ? lazyBlockIterator.next() : null;
    }

    @Override
    public Iterator<String> iterator() {
        return lazyBlockIterator;
    }

    private Iterator<String> nextBlockLines() {
        if (blocks.hasNext())
            return fetchBlockLines(blocks.next()).iterator();
        else
            return Collections.emptyIterator();
    }

    private List<String> fetchBlockLines(String block) {
    	 ClientConfig config2 = new ClientConfig();
         Client client2 = ClientBuilder.newClient(config2);

         URI baseURI2 = UriBuilder.fromUri(datanode).build();
         WebTarget target2 = client2.target(baseURI2);

         
         byte[] response2 = target2.path("/datanode/" + block)
                 .request()
                 .accept(MediaType.APPLICATION_OCTET_STREAM)
                 .get(byte[].class);
        
        
        byte[] data = response2;
        
        return Arrays.asList(new String(data).split("\\R"));
    }

    private class LazyBlockReader implements Iterator<String> {

        Iterator<String> lines;

        LazyBlockReader() {
            this.lines = nextBlockLines();
        }

        @Override
        public String next() {
            return lines.next();
        }

        @Override
        public boolean hasNext() {
            return lines.hasNext() || (lines = nextBlockLines()).hasNext();
        }
        
       
    }
}

