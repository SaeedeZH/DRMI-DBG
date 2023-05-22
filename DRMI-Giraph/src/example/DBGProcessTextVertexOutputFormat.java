package example;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Default output format for {@link org.apache.giraph.examples
 * .SimpleHopsComputation}
 */
public class DBGProcessTextVertexOutputFormat extends
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
	      
	      
	      
    	  StringBuilder sb = new StringBuilder();
    	  sb.append(vertex.getId());
	      sb.append(';');
	      sb.append(vertex.getValue().getKmer());
	      sb.append(';');
	      sb.append(vertex.getValue().getFreq());
	      sb.append(';');
	      sb.append(vertex.getValue().getInputEdges());
	      sb.append(';');
	      
//		      sb.append(vertex.getValue().getPred());
//		      sb.append(';');
//		      sb.append(vertex.getValue().getRank());
//		      sb.append(';');
//		      sb.append(vertex.getValue().getTag());
//		      sb.append('\t');
	      
//		      sb.append('[');
	      
	      for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {	    	
	    	  sb.append(edge.getTargetVertexId());
	    	  sb.append(",");	        
	      }
	      
	      //remove last ','
	      if (sb.charAt(sb.length()-1) == ',')
	    	  sb.deleteCharAt(sb.length()-1);
	      
//		      sb.append(']');
	      return new Text(sb.toString());
	
	      
	      
	    }
  }
}
