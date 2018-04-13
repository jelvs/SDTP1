package client;

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
import org.glassfish.jersey.client.ClientProperties;

import api.storage.Datanode;

public class DatanodeClient implements Datanode {
	
	
	private URI baseURI;
	
	//final int CONNECT_TIMEOUT = 2000;
	//final int READ_TIMEOUT = 2000;
	
	public DatanodeClient(String url) {
		
		baseURI = UriBuilder.fromUri(url).build();
		
		//config.property(ClientProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
		//config.property(ClientProperties.READ_TIMEOUT, READ_TIMEOUT);
	}
	
	@Override
	public String createBlock(byte[] data) {
		ClientConfig config = new ClientConfig();
		Client client = ClientBuilder.newClient(config);
		WebTarget target = client.target(baseURI);
		String id = null;
		//System.out.println("target : " + target.path("/datanode/").toString());
		Response response2 = target.path("datanode/")
				.request()
				.post(Entity.entity(data, MediaType.APPLICATION_OCTET_STREAM));

		if (response2.hasEntity()) {
			id = response2.readEntity(String.class);
		} else
			System.err.println(response2.getStatus());
		
		
		return id;
	}

	@Override
	public void deleteBlock(String block) {
		ClientConfig config = new ClientConfig();
		Client client = ClientBuilder.newClient(config);
		WebTarget target = client.target(baseURI);
		
		
		Response response3 = target.path("datanode/" + block).queryParam("block", block)
				.request()
				.delete();
		if (response3.getStatusInfo().equals(Response.Status.NO_CONTENT)) {
			System.out.println("deleted block resource...");
		} else
			System.err.println(Response.Status.NO_CONTENT);
	}
		

	@Override
	public byte[] readBlock(String block) {
		ClientConfig config = new ClientConfig();
		Client client = ClientBuilder.newClient(config);
		WebTarget target = client.target(baseURI);
		
		//System.out.println("\n BLOCK::    " + block);
		Response response = target.path("datanode/" + block)
				.request()
				.get();

		return response.readEntity(byte[].class);
	}

}
