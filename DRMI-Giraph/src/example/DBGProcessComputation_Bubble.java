package example;


import com.wantedtech.common.xpresso.strings.SequenceMatcher;


import java.io.IOException;

import org.apache.curator.shaded.com.google.common.collect.Iterables;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class DBGProcessComputation_Bubble extends BasicComputation<LongWritable, 
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
			
			if ((inputEdgeNum == 1) && (outputEdgeNum == 1)) {
				
				for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges())
					sendMessage(edge.getTargetVertexId(), new DBGProcessMessage(this.vertex.getId().get(), this.vertex.getValue().getKmer(), this.vertex.getValue().getFreq(), this.vertex.getValue().getInputEdges()));			
			}
			 		
		}
		
		else if (super.getSuperstep() % 2 == 0) {
			int messagesSize = Iterables.size(messages); 
			if (messagesSize == 2) {
//				System.out.println("two message recieved. bubble candid");
				DBGProcessMessage msg1 = new DBGProcessMessage();
				DBGProcessMessage msg2 = new DBGProcessMessage();
				
//				int cnt = 0;
//				for (DBGProcessMessage msg : messages) {				
//					cnt++;
//					if (cnt == 1) {
//						msg1 = new DBGProcessMessage();
//						msg1 = msg;
//					}
//					else
//						msg2 = msg;					
//				}
			
				msg1 = Iterables.get(messages, 0);
				msg2 = Iterables.get(messages, 1);
//				System.out.println("msg1:" + msg1.getSourceId() + "," + msg1.getKmer() + "," + msg1.getFreq() + "," + msg1.getInputEdges());
//				System.out.println("msg2:" + msg2.getSourceId() + "," + msg2.getKmer() + "," + msg2.getFreq() + "," + msg2.getInputEdges());
//				System.out.println("msg.getInputEdges().get(0): " + msg1.getInputEdges().get(0) + ", " + msg2.getInputEdges().get(0));
//				System.out.println((msg1.getInputEdges().get(0) == msg2.getInputEdges().get(0)));
//				System.out.println(msg1.getInputEdges() == msg2.getInputEdges());
//				System.out.println("msg1.getInputEdges().equals(msg2.getInputEdges()): " + msg1.getInputEdges().equals(msg2.getInputEdges()));
						
//				if ((msg1.getInputEdges().get(0)) == (msg2.getInputEdges().get(0))) {
				if (msg1.getInputEdges().equals(msg2.getInputEdges())) {
					
//					System.out.println("two nodes that were sent message have a similar input node, Bubble is detected");
//					System.out.println("msg1:" + msg1.getSourceId() + "," + msg1.getKmer() + "," + msg1.getFreq() + "," + msg1.getInputEdges());
//					System.out.println("msg2:" + msg2.getSourceId() + "," + msg2.getKmer() + "," + msg2.getFreq() + "," + msg2.getInputEdges());
					
//					SequenceMatcher sm = new SequenceMatcher(msg1.getKmer(), msg2.getKmer());
//					System.out.println(sm.ratio());
					double simRatio = StringSimilarity.similarity(msg1.getKmer(), msg2.getKmer());
//					System.out.println("Similarity Ratio: " + simRatio);
					
//					if (sm.ratio() > 0.90) {
					if (simRatio > 0.90) {
					
						long rmNode, inputOfRmNode;
						if (msg1.getFreq() < msg2.getFreq()) {
							rmNode = msg1.getSourceId();
							inputOfRmNode = msg1.getInputEdges().get(0);
							
//							System.out.println("node1 should be removed");
						}
							
						else {
							rmNode = msg2.getSourceId();
							inputOfRmNode = msg2.getInputEdges().get(0);
							
//							System.out.println("node2 should be removed");
						}
						
						removeVertexRequest(new LongWritable(rmNode));
						removeEdgesRequest(new LongWritable(inputOfRmNode), new LongWritable(rmNode));
						this.vertex.getValue().delNodeFromInputEdges(rmNode);
						
//						System.out.println("### Bubble removed ###");
						aggregate("tipCountAgg", new IntWritable(1));
					}
//					else
//						System.out.println("similiratiy of nodes is less than 0.9");
					
							
				}
			}
						
		}
		
	}

}
