package example;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.python.modules.thread.thread;

public class DBGProcessComputation_Tip extends BasicComputation<LongWritable, 
DBGProcessVertexValue, NullWritable, DBGProcessMessage>{
	
	
	/**
	 * The current vertex
	 */
	private Vertex<LongWritable, DBGProcessVertexValue, NullWritable> vertex;	
	
	/**
	 * The broadcast variable that show the kmerSize
	 */
	private LongWritable kmerSize;
	
	@Override
	public void compute(Vertex<LongWritable, DBGProcessVertexValue, NullWritable> vertex, 
			Iterable<DBGProcessMessage> messages) throws IOException {
		
		this.vertex = vertex;	
		
		this.kmerSize = getBroadcast("brdVar_kmerSize");
		
		int outputEdgeNum = this.vertex.getNumEdges();
		int inputEdgeNum = this.vertex.getValue().getNumInputEdges();
		
		if (super.getSuperstep() % 2 == 1) {
			
			if ((inputEdgeNum == 0) && (outputEdgeNum == 1) && (this.vertex.getValue().getKmer().length() < (2 * (int) this.kmerSize.get()))) {
				removeVertexRequest(this.vertex.getId());
				System.out.println("Tip 0in-1out removeVertexRequest: " + this.vertex.getId() + "\t" + this.vertex.getValue().getPred() + "," + this.vertex.getValue().getRank() + "," + this.vertex.getValue().getTag());
				for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges())
					sendMessage(edge.getTargetVertexId(), new DBGProcessMessage(this.vertex.getId().get()));			
			}
			else if ((inputEdgeNum == 1) && (outputEdgeNum == 0) && (this.vertex.getValue().getKmer().length() < (2 * (int) this.kmerSize.get()))) {
				System.out.println("Tip 1in-0out removeVertexRequest: " + this.vertex.getId() + "\t" + this.vertex.getValue().getPred() + "," + this.vertex.getValue().getRank() + "," + this.vertex.getValue().getTag());
				removeVertexRequest(this.vertex.getId());
				long inputId = this.vertex.getValue().getInputEdges().get(0);
				removeEdgesRequest(new LongWritable(inputId), this.vertex.getId());
			
			}
			 		
		}
		
		else if (super.getSuperstep() % 2 == 0) {
			for (DBGProcessMessage msg : messages) {
				this.vertex.getValue().delNodeFromInputEdges(msg.getSourceId());				
			}
						
		}
		
	}

}
