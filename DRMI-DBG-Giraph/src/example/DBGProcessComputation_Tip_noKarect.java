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
import org.python.antlr.PythonParser.break_stmt_return;
import org.python.modules.thread.thread;

public class DBGProcessComputation_Tip_noKarect extends BasicComputation<LongWritable, 
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
//				removeVertexRequest(this.vertex.getId());
//				System.out.println("this.kmerSize: " + this.kmerSize);
//				System.out.println("this.kmerSize.get(): " + this.kmerSize.get());
//				System.out.println("Candid Tip 0in-1out: " + this.vertex.getId());
				for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges())
					sendMessage(edge.getTargetVertexId(), new DBGProcessMessage(this.vertex.getId().get()));
				
				aggregate("tipCountAgg", new IntWritable(1));
			}
			else if ((inputEdgeNum == 1) && (outputEdgeNum == 0) && (this.vertex.getValue().getKmer().length() < (2 * (int) this.kmerSize.get()))) {
//				System.out.println("Candid Tip 1in-0out: " + this.vertex.getId());
//				removeVertexRequest(this.vertex.getId());
				long inputId = this.vertex.getValue().getInputEdges().get(0);
				sendMessage(new LongWritable(inputId), new DBGProcessMessage(this.vertex.getId().get()));
//				removeEdgesRequest(new LongWritable(inputId), this.vertex.getId());
				
				aggregate("tipCountAgg", new IntWritable(1));
			
			}
			 		
		}
		
		else if (super.getSuperstep() % 2 == 0) {
			
			for (DBGProcessMessage msg : messages) {
				
				// Exception case: if only two node by one edge are connected (a->b)
				//this case accrues in Datasets without correction(no karect)
				if ((inputEdgeNum == 1) && (outputEdgeNum == 0)) {
//					long inputId = this.vertex.getValue().getInputEdges().get(0);
//					if (inputId == msg.getSourceId())
					break;
				}
				
				else if ((inputEdgeNum == 0) && (outputEdgeNum == 1)) {
//					for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges())
//						if (edge.getTargetVertexId().get() == msg.getSourceId())
					break;
					
				}
				
				else {
					removeVertexRequest(new LongWritable(msg.getSourceId()));
					boolean isInOutputs = false;
					for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges())
						if (edge.getTargetVertexId().get() == msg.getSourceId()) {
//							System.out.println("Tip 1in-0out removeVertexRequest: " + msg.getSourceId() );
							this.vertex.removeEdges(new LongWritable(msg.getSourceId()));
							isInOutputs = true;
							break;
						}
					if (! isInOutputs) {
//						System.out.println("Tip 0in-1out removeVertexRequest: " + msg.getSourceId() );
						this.vertex.getValue().delNodeFromInputEdges(msg.getSourceId());
					}
					
					aggregate("tipCountAgg", new IntWritable(1));
							
				}
				
			}
						
		}
		
	}

}
