package example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


/**
 * Default input format for {@link org.apache.giraph.DBGProcessComputation2
 * .DBGProcessComputation}
 */
public class DBGProcessTextVertexInputFormat extends TextVertexInputFormat<LongWritable, DBGProcessVertexValue, NullWritable>{
	
	@Override
	public TextVertexInputFormat<LongWritable, DBGProcessVertexValue, NullWritable>.TextVertexReader createVertexReader(
			InputSplit split, TaskAttemptContext context)
			throws IOException {
		// TODO Auto-generated method stub
		return new DBGProcessVertexReaderFromEachLine();
	}

//	@Override
//	public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
////		return new DBGProcessVertexReaderFromEachLine();
//		return null;
//    }

  /**
   * Reads the line and parses them by the following schema:
   * vertexId \t kmer;freq;delimited input id's with , \t delimited output id's with ,
   */
//   * vertexID \t <code>; delimited id's for finding hop counts</code> \t
//   * <code>; delimited id's of neighbors</code>
//   */
	private class DBGProcessVertexReaderFromEachLine extends TextVertexReaderFromEachLine {

		private long id;	
		
	    @Override
	    protected LongWritable getId(Text line) throws IOException {
	      String[] splitLine = line.toString().split(",");
//	      long id = Long.parseLong(splitLine[0]);
	      this.id = Long.valueOf(splitLine[0]).longValue();
	      return new LongWritable(this.id);
	    }
	
		@Override
		protected DBGProcessVertexValue getValue(Text line) throws IOException {
		  DBGProcessVertexValue value = new DBGProcessVertexValue();
		  String[] splitLine = line.toString().split(",");
		  
		  String ValueParams;
//		  if (this.id <= 100000000) 		  
		  ValueParams = splitLine[1] + "," + splitLine[2];
//		  else
//			  ValueParams = splitLine[1] + "," + 1;
		  		  
		  value.initializeValueParams(ValueParams);
		  
		  return value;
		}
		
		@Override
		protected Iterable<Edge<LongWritable, NullWritable>> getEdges(
		        Text line) throws IOException {
			
			String[] splitLine = line.toString().split(",");
			
			List<Edge<LongWritable, NullWritable>> edges = new
		              ArrayList<Edge<LongWritable, NullWritable>>();
			String str = "\"\"";
			
			int startEdgeIndex = 3;
//			if (this.id <= 100000000)
//				startEdgeIndex = 3;
//			else
//				startEdgeIndex = 2;
			
			for (int i = startEdgeIndex; i < splitLine.length; i++) {
//				if (splitLine[i].matches(str)) {
//					break;
//				}
//				else {
//					long targetId = Long.parseLong(splitLine[i]);
//					edges.add(EdgeFactory.create(new LongWritable(targetId)));
//				}
				
				
				if (splitLine[i].matches(str)) {
					continue;
				}
				else if (splitLine[i].startsWith("\"")){
				        splitLine[i] = splitLine[i].substring(1, splitLine[i].length());
				}
			    else if (splitLine[i].endsWith("\"")){
			        splitLine[i] = splitLine[i].substring(0, splitLine[i].length()-1);
			    }
			  
				long targetId = Long.parseLong(splitLine[i].trim());
				edges.add(EdgeFactory.create(new LongWritable(targetId)));
//				System.out.println("num: " + targetId);					
	      }
	
	      return edges;
					
		}
	}

}
