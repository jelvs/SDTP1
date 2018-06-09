package test.blobstorage;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobReader;
import api.storage.BlobStorage.BlobWriter;
import sys.storage.RestBlobStorage;

public class LocalBlobStorageTest {		
		
		/*
		 * Exemplifies the use of BlobStorage by Applications (such as the MapReduce engine)
		 * 
		 */
		public static void main(String[] args) throws Exception {

			System.setProperty("java.net.preferIPv4Stack", "true");
			
			System.out.println("\n teste1 \n");
			//1. Get an implementation of the storage. LocalBlobStorage implements everything locally, in memory, sadly...
			BlobStorage storage = new RestBlobStorage();
			
			System.out.println("\n teste2 \n");
			//2. We can list ALL the blobs already stored in the storage, using "" as the name prefix.
			List<String> blobs = storage.listBlobs("");
			
			assert blobs.isEmpty(); //At this stage we expect an empty list.
			
			System.out.println("\n teste3 \n");
			//3. Write a blob containing a single line of text. We must not forget to close the blob, at the end.
			BlobWriter writer = storage.blobWriter("1-line-blob");
			writer.writeLine("it works!");
			writer.close();
			
			System.out.println("\n teste4 \n");
			//4. Listing again, should return 1 and only blob.
			assert storage.listBlobs("").size() == 1;
			
			System.out.println("\n teste5 \n");
			//5. Read the contents of the blob to the standard output.
			BlobReader reader = storage.readBlob("1-line-blob");
			for( String line : reader )
				System.out.println( line  );
			
			System.out.println("\n teste6 \n");
			//6. Read the contents of the blob to the standard output, Java 8+ way.
			storage.readBlob("1-line-blob").forEach( System.out::println );
			
			System.out.println("\n teste7 ñ");
			//7. Read a text file and save it as a blob to the storage:			
			BlobWriter writer2 = storage.blobWriter("WordCount");
			for( String line : Files.readAllLines(new File("WordCount.java").toPath()))
				writer2.writeLine( line );
			writer2.close();
			
			System.out.println("\n teste8 \n");
			//8. Read a text file and save it as a blob to the storage. Java 8+ way		
			BlobWriter writer3 = storage.blobWriter("WordCount2");
			Files.readAllLines(new File("WordCount.java").toPath()).stream().forEach( writer3::writeLine );
			writer3.close();
			
			System.out.println("\n teste9 \n");
			//9. Read the contents of blobs, whose names start with "Word" to the standard output. Java 8+ way.
			storage.listBlobs("Word").forEach( blob -> {
				storage.readBlob( blob ).forEach( System.out::println );
			});
			
			System.out.println("\n teste10 \n");
			//10. Delete all blobs starting with "W"
			storage.deleteBlobs("W");

			System.out.println("\n teste11 \n");
			//11. Delete all blobs found in storage.
			storage.deleteBlobs("");
		}
}
