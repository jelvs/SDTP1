package api.mapreduce;

import javax.jws.WebMethod;
import javax.jws.WebService;

@WebService
public interface ComputeNode {
		static final String PATH = "/mapreduce";	
		static final String NAME="ComputeService";
		static final String NAMESPACE="http://sd2018";
		static final String INTERFACE="api.mapReduce.ComputeNode";

		@WebMethod
		void mapper( String jobClassBlob, String inputPrefix , String outputPrefix );
		
		@WebMethod
		void reducer( String jobClassBlob, String inputPrefix , String outputPrefix );
		
		@WebMethod
		void mapReduce( String jobClassBlob, String inputPrefix , String outputPrefix );
}
