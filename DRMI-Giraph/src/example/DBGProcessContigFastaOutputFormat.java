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
public class DBGProcessContigFastaOutputFormat extends
        TextVertexOutputFormat<LongWritable, DBGProcessVertexValue,
                NullWritable> {
	
	@Override
	public TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new DBGProcessTextVertexLineWriter();
	}

   /**
   * Outputs in fasta format, for each line the vertex id and ...
   */
	private class DBGProcessTextVertexLineWriter extends 
		TextVertexWriterToEachLine {

	    @Override
	    protected Text convertVertexToLine(
	            Vertex<LongWritable, DBGProcessVertexValue,
	                    NullWritable> vertex) throws IOException {
	    	String strFatsa = "";
	    	if ((vertex.getValue().getKmer().length() > 200 ) || (vertex.getValue().getTag() == (byte)10)) {
	    		strFatsa = ">";
		    	strFatsa += vertex.getId().get();
		    	// Branch contigs tag parameter is 10
		    	if (vertex.getValue().getTag() == (byte)10) {
		    		strFatsa += " @" + vertex.getValue().getPred() + "," + vertex.getValue().getRank();
		    		for(long frq : vertex.getValue().getInputEdges()) {
		    			strFatsa += "," + frq;
		    		}
		    	}
		    	else {
		    		strFatsa += " freq: " + vertex.getValue().getFreq();
		    	}
		    	strFatsa += "\n";
		    	strFatsa += vertex.getValue().getKmer();
		    	return new Text(strFatsa);
	    	}	    	
	    	
	    	return null;
	    	
	    }
  }
}
