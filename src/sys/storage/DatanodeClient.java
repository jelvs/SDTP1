package sys.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import api.storage.Datanode;
import utils.Random;

/*
 * Fake Datanode client.
 * 
 * Rather than invoking the Datanode via REST, executes
 * operations locally, in memory.
 * 
 */
public class DatanodeClient implements Datanode {
	private static Logger logger = Logger.getLogger(Datanode.class.toString() );

	private static final int INITIAL_SIZE = 32;
	private Map<String, byte[]> blocks = new HashMap<>(INITIAL_SIZE);
	
	@Override
	public String createBlock(byte[] data) {
		String id = Random.key64();
		blocks.put( id, data);
		return id;
	}

	@Override
	public void deleteBlock(String block) {
		blocks.remove(block);
	}

	@Override
	public byte[] readBlock(String block) {
		byte[] data =  blocks.get(block);
		if( data != null )
			return data;
		else
			throw new RuntimeException("NOT FOUND");
	}
}
