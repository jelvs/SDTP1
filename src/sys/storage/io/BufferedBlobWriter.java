package sys.storage.io;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import api.storage.BlobStorage.BlobWriter;
import api.storage.Datanode;
import api.storage.Namenode;
import org.glassfish.jersey.client.ClientConfig;
import utils.IO;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

/*
 *
 * Implements a ***centralized*** BlobWriter.
 *
 * Accumulates lines in a list of blocks, avoids splitting a line across blocks.
 * When the BlobWriter is closed, the Blob (and its blocks) is published in the Namenode.
 *
 */
public class BufferedBlobWriter implements BlobWriter {
	final String name;
	final int blockSize;
	final ByteArrayOutputStream buf;

	final String namenode;
	final String[] datanodes;
	final List<String> blocks = new LinkedList<>();

	public BufferedBlobWriter(String name, String namenode, String[] datanodes, int blockSize ) {
		this.name = name;
		this.namenode = "http://localhost:9091";
		this.datanodes = new String[]{"http://localhost:9999"};

		this.blockSize = blockSize;
		this.buf = new ByteArrayOutputStream( blockSize );
	}

	private void flush( byte[] data, boolean eob ) {

		ClientConfig config = new ClientConfig();
		Client client = ClientBuilder.newClient(config);

		URI baseURI = UriBuilder.fromUri(datanodes[0]).build();
		WebTarget target = client.target( baseURI );

		Response response = target.path("/datanode/")
				.request()
				.post( Entity.entity( new byte[1024], MediaType.APPLICATION_OCTET_STREAM));

		if( response.hasEntity() ) {
			String id = response.readEntity(String.class);
			System.out.println( "data resource id: " + id );
            blocks.add(id);
		} else
			System.err.println( response.getStatus() );



		if( eob ) {

			ClientConfig config2 = new ClientConfig();
			Client client2 = ClientBuilder.newClient(config);

			URI baseURI2 = UriBuilder.fromUri(namenode).build();
			WebTarget target2 = client.target( baseURI );

			Response response2 = target.path("/namenode/" + name )
					.request()
					.post( Entity.entity( blocks , MediaType.APPLICATION_JSON_TYPE));

			if( response.hasEntity() ) {
				String id = response.readEntity(String.class);
				System.out.println( "data resource id: " + id );
			} else
				System.err.println( response.getStatus() );


			blocks.clear();
		}

		/*blocks.add( datanodes[0].createBlock(data)  );
		if( eob ) {
			namenode.create(name, blocks);
			blocks.clear();
		}*/
	}

	@Override
	public void writeLine(String line) {
		if( buf.size() + line.length() > blockSize - 1 ) {
			this.flush(buf.toByteArray(), false);
			buf.reset();
		}
		IO.write( buf, line.getBytes() );
		IO.write( buf, '\n');
	}

	@Override
	public void close() {
		flush( buf.toByteArray(), true );
	}
}