package sys.storage;


import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import api.storage.Datanode;
import utils.JSON;
import utils.Random;

/*
 * Fake Datanode client.
 * 
 * Rather than invoking the Datanode via REST, executes
 * operations locally, in memory.
 * 
 */
public class DatanodeResources implements Datanode {
	private static Logger logger = Logger.getLogger(Datanode.class.toString() );

	private static final int INITIAL_SIZE = 32;
	private Map<String, byte[]> blocks = new HashMap<>(INITIAL_SIZE);


	private URI baseURI;
	private String g;


	public DatanodeResources(URI baseURI) throws IOException {

		this.baseURI = baseURI;
		g = System.getProperty("user.dir");
	}


	@Override
	public synchronized String createBlock(byte[] data) {
		String id = Random.key64();
		blocks.put( id, data);
		try {
			Files.write(Paths.get(g + "/" + id), data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return baseURI + "datanode/" + id;

	}
	/*
	@Override
	public synchronized void blocksToKeep(List<String> keepTHISblocks) { 
		Map<String, byte[]> newBlocks = new HashMap<>();
		for (String element  : keepTHISblocks) {
			byte[] x = blocks.get(element);
			if(x != null) {
				newBlocks.put(element, x);
			}
		}
		blocks = newBlocks;	
		
		throw new WebApplicationException(Status.CONFLICT);
	}
*/




@Override
public synchronized void deleteBlock(String block) {
	try {
		if(!Files.deleteIfExists(Paths.get(g + "/" + block))) {
			throw new WebApplicationException(Status.NOT_FOUND);	
		}
	} catch (IOException e) {
		e.printStackTrace();


	}throw new WebApplicationException(Status.NO_CONTENT);



}


@Override
public synchronized byte[] readBlock(String block) {
	try {

		if(Files.exists(Paths.get(g + "/" + block))){
			return Files.readAllBytes(Paths.get(g + "/" + block));

		}else {
			throw new WebApplicationException(Status.NOT_FOUND);
		}
	} catch (IOException e) {
		e.printStackTrace();
		return null;
		// TODO Auto-generated catch block

	}


}
}

