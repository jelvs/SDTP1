package sys.storage;

import java.io.BufferedWriter;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

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
	private URI baseURI;


	public DatanodeResources(URI baseURI) throws IOException {
		/*yourFile = new File("/Datanodes");
		if(!yourFile.exists()) {
			new yourFile("/Datanodes").mkdir();
			
		}*/
		this.baseURI=baseURI;
		
	}

	public void writetoFile(String id, byte[] data) {

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
		/*if(!yourFile.exists()) {
			writetoFile(id, data);
		}else{
			id = Random.key64();
			writetoFile(id, data);
		}*/
		return baseURI + "/datanode/" + id;
	}

	@Override
	public void deleteBlock(String block) {
		blocks.remove(block);
	}

	@SuppressWarnings("resource")
	@Override
	public byte[] readBlock(String block) {
		byte[] data =  blocks.get(block);
		if( data != null )
			return data;
		else
			throw new RuntimeException("NOT FOUND");
		}
	/*RandomAccessFile f;
		byte[] b;
		
		if(yourFile.exists()){
			yourFile = yourFile.getParentFile();
			if(yourFile.toString() == block) {
				try {
					f = new RandomAccessFile(yourFile, "r");
					b = new byte[(int)f.length()];
					f.readFully(b);  
					return b;

				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return new byte[0];
	}*/
}
