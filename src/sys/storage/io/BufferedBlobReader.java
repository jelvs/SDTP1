package sys.storage.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import api.storage.BlobStorage.BlobReader;
import api.storage.Datanode;
import api.storage.Namenode;
import client.DatanodeClient;

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
	final Namenode namenode; 
	final ConcurrentHashMap<String, DatanodeClient> datanodes;

	final Iterator<String> blocks;

	final LazyBlockReader lazyBlockIterator;

	public BufferedBlobReader( String name, Namenode namenode, ConcurrentHashMap<String, DatanodeClient> datanodes) {
		this.name = name;
		this.namenode = namenode;
		this.datanodes = datanodes;
		this.blocks = this.namenode.read( name ).iterator();
		this.lazyBlockIterator = new LazyBlockReader();
	}

	@Override
	public String readLine() {
		return lazyBlockIterator.hasNext() ? lazyBlockIterator.next() : null ;
	}

	@Override
	public Iterator<String> iterator() {
		return lazyBlockIterator;
	}

	private Iterator<String> nextBlockLines() {
		if( blocks.hasNext() )
			return fetchBlockLines( blocks.next() ).iterator();
		else 
			return Collections.emptyIterator();
	} 

	private List<String> fetchBlockLines(String block) {
		byte[] data  = null;
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
		if(namenode.equals(name)) {
			for(DatanodeClient x : datanodes.values()) {
				//System.out.println("datanodes size : " + datanodes.size());
				data = x.readBlock( block );
				try {
					outputStream.write(data);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}


			}data = outputStream.toByteArray( );}

		return Arrays.asList( new String(data).split("\\R"));


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
