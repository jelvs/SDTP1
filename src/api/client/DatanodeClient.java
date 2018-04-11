package api.client;

import java.net.URI;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import org.glassfish.jersey.client.ClientConfig;

import api.storage.Datanode;

public class DatanodeClient implements Datanode {
	
	private ClientConfig config = new ClientConfig();
	private Client client = ClientBuilder.newClient(config);
	private URI baseURI;
	private WebTarget target;
	
	public DatanodeClient(String url) {
		
		baseURI = UriBuilder.fromUri(url).build();
		target = client.target(baseURI);
	}
	
	@Override
	public String createBlock(byte[] data) {
		
		String id = null;
		Response response2 = target.path("/datanode/")
				.request()
				.post(Entity.entity(data, MediaType.APPLICATION_JSON_TYPE));

		if (response2.hasEntity()) {
			id = response2.readEntity(String.class);
		} else
			System.err.println(response2.getStatus());
		
		return id;
	}

	@Override
	public void deleteBlock(String block) {
		
		Response response3 = target.path("/datanode/" + block).queryParam("block", block)
				.request()
				.delete();
		if (response3.getStatusInfo().equals(Response.Status.NO_CONTENT)) {
			System.out.println("deleted block resource...");
		} else
			System.err.println(response3.getStatus());

	}
		

	@Override
	public byte[] readBlock(String block) {
		
		Response response = target.path("/datanode/" + block)
				.request()
				.get();

		return response.readEntity(byte[].class);
	}

}
