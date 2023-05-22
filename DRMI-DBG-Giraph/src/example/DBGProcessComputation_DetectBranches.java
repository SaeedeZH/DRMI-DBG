package example;

import java.io.IOException;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;


public class DBGProcessComputation_DetectBranches extends BasicComputation<LongWritable, 
DBGProcessVertexValue, NullWritable, DBGProcessMessage>{
	
	
	/**
	 * The current vertex
	 */
	private Vertex<LongWritable, DBGProcessVertexValue, NullWritable> vertex;	
	
	
	@Override
	public void compute(Vertex<LongWritable, DBGProcessVertexValue, NullWritable> vertex, 
			Iterable<DBGProcessMessage> messages) throws IOException {
		
		this.vertex = vertex;	
		
		
		int outputEdgeNum = this.vertex.getNumEdges();
		int inputEdgeNum = this.vertex.getValue().getNumInputEdges();
				
		if (super.getSuperstep() % 2 == 1) {
			
			if ((inputEdgeNum == 2) && (outputEdgeNum == 2)) {
				
				// Vertices that have 2 input and 2 outputs, are in middle of a Branch
				// set 'tag' property of Branch middle node to 6 
				this.vertex.getValue().setTag((byte) 6);
				/**
				 * Vertices that have 2 input and 2 outputs, are in middle of a Branch 
				 * So this vertex send message '1' or '2' to inputs and '4' or '5' to outputs as an index
				 * we use 'rank' parameter as index in branch 		
				 * index tell us node situation in branch.
				 * rank = 1 or 2 for input nodes
				 * rank = 4 or 5 for output nodes
				 */
				int index = 1;
				for (Long inputId : this.vertex.getValue().getInputEdges()) {
					sendMessage(new LongWritable(inputId), new DBGProcessMessage(this.vertex.getId().get(), index));
					index++;
				}
				
				for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges()) {
					index++;
					sendMessage(edge.getTargetVertexId(), new DBGProcessMessage(this.vertex.getId().get(), index));					
				}				
	
				aggregate("tipCountAgg", new IntWritable(1));		 		
			}
			
		}
		
		else {
		
			for (DBGProcessMessage msg : messages) {
				//set 'pred' property as Branch node Id 
				this.vertex.getValue().setPred(msg.getSourceId());
				// set 'Rank' property as index of nodes in branch
				// index is node situation in branch.
				// rank = 1 or 2 for input nodes
				// rank = 4 or 5 for output nodes
				this.vertex.getValue().setRank(msg.getRank());
				// set 'Tag' property to '5' to show node is in branch
				// for using 'pred' and 'rank' property as branch parameters 
				this.vertex.getValue().setTag((byte) 5); 

			}
			
						
		}
		
	}

}
