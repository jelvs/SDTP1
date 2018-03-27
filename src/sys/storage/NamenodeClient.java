package sys.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;

import api.storage.Namenode;

/*
 * Fake NamenodeClient client.
 * 
 * Rather than invoking the Namenode via REST, executes
 * operations locally, in memory.
 * 
 * Uses a trie to perform efficient prefix query operations.
 */
public class NamenodeClient implements Namenode {

	private static Logger logger = Logger.getLogger(NamenodeClient.class.toString() );
	
	Trie<String, List<String>> names = new PatriciaTrie<>();
	
	@Override
	public List<String> list(String prefix) {
		return new ArrayList<>(names.prefixMap( prefix ).keySet());
	}

	@Override
	public void create(String name,  List<String> blocks) {
		if( names.putIfAbsent(name, new ArrayList<>(blocks)) != null )
			logger.info("CONFLICT");
	}

	@Override
	public void delete(String prefix) {
		List<String> keys = new ArrayList<>(names.prefixMap( prefix ).keySet());
		if( ! keys.isEmpty() )
			names.keySet().removeAll( keys );
	}

	@Override
	public void update(String name, List<String> blocks) {
		if( names.putIfAbsent( name, new ArrayList<>(blocks)) == null ) {
			logger.info("NOT FOUND");
		}
	}

	@Override
	public List<String> read(String name) {
		List<String> blocks = names.get( name );
		if( blocks == null )
			logger.info("NOT FOUND");
		return blocks;
	}
}
