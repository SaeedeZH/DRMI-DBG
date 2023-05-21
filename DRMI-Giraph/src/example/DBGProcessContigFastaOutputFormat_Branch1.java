package example;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.wantedtech.common.xpresso.types.str;

/**
 * Default output format for {@link org.apache.giraph.examples
 * .SimpleHopsComputation}
 */
public class DBGProcessContigFastaOutputFormat_Branch1 extends
        TextVertexOutputFormat<LongWritable, DBGProcessVertexValue,
                NullWritable> {
	
	@Override
	public TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new DBGProcessTextVertexLineWriter();
	}

   /**
   * Outputs for each line the vertex id and the searched vertices with their
   * hop count
   */
	private class DBGProcessTextVertexLineWriter extends 
		TextVertexWriterToEachLine {

	    @Override
	    protected Text convertVertexToLine(
	            Vertex<LongWritable, DBGProcessVertexValue,
	                    NullWritable> vertex) throws IOException {
	    	String strFatsa = "";
	    	if (vertex.getValue().getKmer().length() > 200 ) {
	    		strFatsa = ">";
		    	strFatsa += vertex.getId().get();
		    	// for show nodes in branch in this format: >node_id @rank,branch_node_id
		    	if (vertex.getValue().getTag() == (byte) 6) {
		    		//set 3 as branch node rank
		    		strFatsa += " @3,";
		    		for (Long inputId : vertex.getValue().getInputEdges()) {
		    			strFatsa += inputId;
		    			strFatsa += ",";
					}
					
					for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
						strFatsa += edge.getTargetVertexId().get();
						strFatsa += ",";
					}	
		    	}
		    	
		    	else if (vertex.getValue().getTag() == (byte) 5) {
		    		strFatsa += " @" + vertex.getValue().getRank() + ",";
			    	strFatsa += vertex.getValue().getPred();
		    	}
		    	strFatsa += "\n";
		    	strFatsa += vertex.getValue().getKmer();
		    	return new Text(strFatsa);
	    	}
	    	
	    	// for nodes in branch with length < 200
	    	else {
		    	
	    		// tag=6 for branch middle node, tag=5 for other nodes in branch
		    	if ((vertex.getValue().getTag() == (byte) 6) || (vertex.getValue().getTag() == (byte) 5)) {
		    		strFatsa = ">";
			    	strFatsa += vertex.getId().get();
			    	if (vertex.getValue().getTag() == (byte) 6) {
			    		//set 3 as branch node rank
			    		strFatsa += " @3,";
			    		for (Long inputId : vertex.getValue().getInputEdges()) {
			    			strFatsa += inputId;
			    			strFatsa += ",";
						}
						
						for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
							strFatsa += edge.getTargetVertexId().get();
							strFatsa += ",";
						}	
			    	}
			    	
			    	else if (vertex.getValue().getTag() == (byte) 5) {
			    		strFatsa += " @" + vertex.getValue().getRank() + ",";
				    	strFatsa += vertex.getValue().getPred();
			    	}
			    	strFatsa += "\n";
			    	strFatsa += vertex.getValue().getKmer();
			    	return new Text(strFatsa);
		    	}
	    		
	    	}
	    	
	    	
	    	return null;
	    	
	    }
  }
}
