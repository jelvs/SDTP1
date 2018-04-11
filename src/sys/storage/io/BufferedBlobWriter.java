package sys.storage.io;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import api.storage.BlobStorage.BlobWriter;
import client.DatanodeClient;
import client.NamenodeClient;
import api.storage.Datanode;
import api.storage.Namenode;
import utils.IO;

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

	NamenodeClient namenode; 
	final ConcurrentHashMap<String, DatanodeClient> datanodes;
	final List<String> blocks = new LinkedList<>();

	public BufferedBlobWriter(String name, NamenodeClient namenode, ConcurrentHashMap<String, DatanodeClient> datanodes, int blockSize ) {
		this.name = name;
		this.namenode = namenode;
		this.datanodes = datanodes;

		this.blockSize = blockSize;
		this.buf = new ByteArrayOutputStream( blockSize );
	}

	private void flush( byte[] data, boolean eob ) {
		for (Entry<String, DatanodeClient> url : datanodes.entrySet()) {
			blocks.add(datanodes.get(url.getKey()).createBlock(data));
			if( eob ) {
				namenode.create(name, blocks);
				blocks.clear();
			}
		}
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