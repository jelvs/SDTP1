package sys.storage.io;

import java.io.ByteArrayOutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import api.storage.BlobStorage.BlobWriter;
import client.DatanodeClient;
import client.NamenodeClient;
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
	DatanodeClient[]  datanodess;
	final List<String> blocks = new LinkedList<>();

	public BufferedBlobWriter(String name, NamenodeClient namenode, DatanodeClient[] datanodess, int blockSize ) {
		this.name = name;
		this.namenode = namenode;
		this.datanodess = datanodess;
		this.blockSize = blockSize;
		this.buf = new ByteArrayOutputStream( blockSize );
	}

	private void flush( byte[] data, boolean eob ) {
		/*int randInt = 0;
		Random rand = new Random();
		if(datanodess.length > 1) {
		randInt = rand.nextInt( datanodess.length);
		}
		
		blocks.add(datanodess[randInt].createBlock(data));
		*/
		int randInt;
		Random rand = new Random();
		try {
			 randInt = rand.nextInt( datanodess.length);
		}catch( IllegalArgumentException e){
			 randInt = 0;
		}
		blocks.add(datanodess[randInt].createBlock(data));
		if( eob ) {
			namenode.create(name, blocks);
			blocks.clear();
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