package test;

import java.io.File;
import java.nio.file.Files;

import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobWriter;
import sys.mapreduce.MapReduceEngine;
import sys.storage.LocalBlobStorage;
import utils.Random;

public class LocalMapReduceTest {		
		
		/*
		 * 1) Creates a local (in memory) storage to store blobs in memory.
		 * 
		 * 2) Creates a blob in local storage with the MapReduce program: WordCount.java
		 * 
		 * 3) Creates a blob in local storage with a text document: "lusiadas.txt"
		 * 
		 * 4) Executa o WordCount sobre os blobs contendo os lusiadas e escreve os resultados num blob.
		 *    
		 *    
		 *  If instead of a LocalBlobStorage you implement a RemoteBlobStorage,
		 *  that uses the Namenode e Datanodes to store the blobs, the MapReduce
		 *  computation should run without any changes.
		 */
		public static void main(String[] args) throws Exception {

			BlobStorage storage = new LocalBlobStorage();
			
			BlobWriter src = storage.blobWriter("WordCount");
			Files.readAllLines(new File("WordCount.java").toPath())
				.stream().forEach( src::writeLine );
			src.close();

			for( String doc : new String[] {"doc-1", "doc-2"}) {
				BlobWriter out = storage.blobWriter(doc);
				Files.readAllLines(new File(doc + ".txt").toPath())
				.stream().forEach( out::writeLine );
				out.close();
			}
			
			storage.listBlobs("doc-").stream().forEach( blob -> {
				storage.readBlob(blob).forEach( System.out::println );
			});
			
			storage.deleteBlobs("results-");
			
			String jobID = Random.key64();
			String outputBlob = "results-" + jobID;
			
			MapReduceEngine engine = new MapReduceEngine( "local", storage);
			engine.executeJob("WordCount", "doc-", outputBlob);
					
			storage.listBlobs(outputBlob).stream().forEach( blob -> {
				System.out.println(blob);
				storage.readBlob(blob).forEach( System.out::println );
			});			
		}
}
