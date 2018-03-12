package sys.storage;

import java.util.List;

import api.storage.BlobStorage;
import api.storage.Datanode;
import api.storage.Namenode;
import sys.storage.io.BufferedBlobReader;
import sys.storage.io.BufferedBlobWriter;

public class LocalBlobStorage implements BlobStorage {
	private static final int BLOCK_SIZE=512;

	Namenode namenode;
	Datanode[] datanodes;

	public LocalBlobStorage() {
		this.namenode = new NamenodeClient();
		this.datanodes = new Datanode[] { new DatanodeClient() };
	}

	@Override
	public List<String> listBlobs(String prefix) {
		return namenode.list(prefix);
	}

	@Override
	public void deleteBlobs(String prefix) {
		namenode.list( prefix ).forEach( blob -> {
			namenode.read( blob ).forEach( block -> {
				datanodes[0].deleteBlock(block);
			});
		});
		namenode.delete(prefix);
	}

	@Override
	public BlobReader readBlob(String name) {
		return new BufferedBlobReader( name, namenode, datanodes[0]);
	}

	@Override
	public BlobWriter blobWriter(String name) {
		return new BufferedBlobWriter( name, namenode, datanodes, BLOCK_SIZE);
	}
}
