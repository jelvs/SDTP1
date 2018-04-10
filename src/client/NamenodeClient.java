package client;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.glassfish.jersey.client.ClientConfig;

import api.storage.Namenode;

public class NamenodeClient implements Namenode {

	private ClientConfig config = new ClientConfig();
	private Client client = ClientBuilder.newClient(config);
	private URI baseURI;
	private WebTarget target;
	
	private static Logger logger = Logger.getLogger(NamenodeClient.class.toString() );
	
	Trie<String, List<String>> names = new PatriciaTrie<>();
	
	public NamenodeClient(String url) {
		
		baseURI = UriBuilder.fromUri(url).build();
		WebTarget target = client.target(baseURI);
	}
	
	@Override
	public List<String> list(String prefix) {
		
		Response response = target.path("/namenode/list/").queryParam("prefix", prefix).request().get();

		if (response.hasEntity()) {
			List<String> data = response.readEntity(List.class);
			return data;
		} else
			System.err.println(response.getStatus());
		return new ArrayList<String>();

		
	}

	@Override
	public void create(String name,  List<String> blocks) {
		Response response2 = target.path("/namenode/" + name)
				.request()
				.post(Entity.entity(blocks, MediaType.APPLICATION_JSON_TYPE));

		if (response2.hasEntity()) {
			String id = response2.readEntity(String.class);
			System.out.println("data resource id: " + id);
		} else
			System.err.println(response2.getStatus());


	}

	@Override
	public void delete(String prefix) {
		Response response3 = target.path("/namenode/list/").queryParam("prefix", prefix).request().delete();
		if (response3.getStatusInfo().equals(Response.Status.NO_CONTENT)) {
			System.out.println("deleted data resource...");
		} else
			System.err.println(response3.getStatus());

	}

	@Override
	public void update(String name, List<String> blocks) {
		Response response = target.path("/namenode/" + name).request().put(Entity.entity(name, MediaType.APPLICATION_JSON_TYPE));
		if( names.putIfAbsent( name, new ArrayList<>(blocks)) == null ) {
			logger.info("NOT FOUND");
		}else
			if(response.hasEntity()) {
				response.readEntity(List.class);
			}
	}

	@Override
	public List<String> read(String name) {
		Response response = target.path("/namenode/" + name)
				.request()
				.get();

		return response.readEntity(List.class);
		
	}
}
