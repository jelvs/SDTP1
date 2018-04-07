package sys.storage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Calendar;
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
public class DatanodeResources implements Datanode {
	private static Logger logger = Logger.getLogger(Datanode.class.toString() );

	private static final int INITIAL_SIZE = 32;
	private Map<String, byte[]> blocks = new HashMap<>(INITIAL_SIZE);
	private File yourFile;

	public DatanodeResources() throws IOException {
		yourFile = new File(String.format("http://" + InetAddress.getLocalHost().getHostAddress() + ":8080/"));
		yourFile.createNewFile(); // if file already exists will do nothing
	}

	public void writetoFile(byte[] data) {

		BufferedWriter writer = null;
		try {
			//create a temporary file
			writer = new BufferedWriter(new FileWriter(yourFile, true));
			writer.write(data.toString());

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				// Close the writer regardless of what happens...
				writer.close();
			} catch (Exception e) {
			}
		}

	}

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
		if( data != null ) {
			writetoFile(data);
			return data;
			
		}else
			throw new RuntimeException("NOT FOUND");
	}
}
