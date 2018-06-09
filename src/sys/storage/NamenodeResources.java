package sys.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

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
public class NamenodeResources implements Namenode {

	private static Logger logger = Logger.getLogger(NamenodeResources.class.toString() );
	
	Trie<String, List<String>> names = new PatriciaTrie<>();
	
	
	@Override
	public synchronized List<String> list(String prefix) {
		return new ArrayList<>(names.prefixMap( prefix ).keySet());
	}
	/*
	@Override
	public synchronized List<String> listAll() {
		return new ArrayList<>(names.keySet());
	}
	
	
	public synchronized	List<String> getAllBLocks() {
		List<List<String>> values = (List<List<String>>) names.values();
		//turns the values into a single list
				List<String> flat =  values.stream()
					        .flatMap(List::stream)
					        .collect(Collectors.toList());
		return flat;
		
	}
	*/
	@Override
	public synchronized void create(String name,  List<String> blocks) {
		if( names.putIfAbsent(name, new ArrayList<>(blocks)) != null )
			throw new WebApplicationException(Status.CONFLICT);
		throw new WebApplicationException(Status.NO_CONTENT);
	}

	@Override
	public synchronized void delete(String prefix) {
		List<String> keys = new ArrayList<>(names.prefixMap( prefix ).keySet());
		if( ! keys.isEmpty() ) {
			names.keySet().removeAll( keys );
			throw new WebApplicationException(Status.NO_CONTENT);
		}throw new WebApplicationException(Status.NOT_FOUND);
	}

	@Override
	public synchronized void update(String name, List<String> blocks) {
		if( names.putIfAbsent( name, new ArrayList<>(blocks)) == null ) {
			throw new WebApplicationException(Status.NOT_FOUND);
		}throw new WebApplicationException(Status.NO_CONTENT);
	}

	@Override
	public synchronized List<String> read(String name) {
		List<String> blocks = names.get( name );
		if( blocks == null )
			throw new WebApplicationException(Status.NOT_FOUND);
		return blocks;
	}
}
