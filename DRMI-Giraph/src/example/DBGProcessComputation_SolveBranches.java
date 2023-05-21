package example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.curator.shaded.com.google.common.collect.Iterables;
import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;


public class DBGProcessComputation_SolveBranches extends BasicComputation<LongWritable, 
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
			
			
			if (Iterables.size(messages) > 0) {
				String node1 = "", node2 = "", node3 = "", node4 = "", node5 = "";
				long freq1 = 0, freq2 = 0, freq4 = 0, freq5 = 0;
				node3 = this.vertex.getValue().getKmer();
				
				System.out.print("Middle Node -> " + this.vertex.getId().get() + " - ");//test
				
				for (DBGProcessMessage msg : messages) {
					
					System.out.print(msg.getRank() + " -> " + msg.getSourceId() + " - ");//test					
					
					switch (msg.getRank()) {
						case 1:
							node1 = msg.getKmer();
							freq1 = msg.getFreq();
							break;
						case 2:
							node2 = msg.getKmer();
							freq2 = msg.getFreq();
							break;
						case 4:
							node4 = msg.getKmer();
							freq4 = msg.getFreq();
							break;
						case 5: 
							node5 = msg.getKmer();
							freq5 = msg.getFreq();
							break;
					}
					
				}
				
				System.out.println("\r\n----------------------------------------");//test
				
				int len1 = node1.length();
				int len2 = node2.length();
				int len4 = node4.length();
				int len5 = node5.length();
				
				try {
					node1 = node1.substring(0, node1.length() - (int) this.kmerSize.get() + 1);
					node2 = node2.substring(0, node2.length() - (int) this.kmerSize.get() + 1);
					node3 = node3.substring(0, node3.length() - (int) this.kmerSize.get() + 1);
	//				node3 = node3.substring((int)this.kmerSize.get()-1, this.vertex.getValue().getKmer().length());
	//				node4 = node4.substring((int)this.kmerSize.get()-1, this.vertex.getValue().getKmer().length());
	//				node5 = node5.substring((int)this.kmerSize.get()-1, this.vertex.getValue().getKmer().length());
				}
				catch (Exception e) {
					// TODO: handle exception
					System.out.println("*********my Error: " + e.toString());
					System.out.println("Node1: " + node1);
					System.out.println("Node2: " + node2);
					System.out.println("Node3: " + node3);
					System.out.println("Node4: " + node4);
					System.out.println("Node5: " + node5);
				}
				
				// a list for keeping freq of first and last nodes of branch
				List<Long> freqList = new ArrayList<Long>();
				freqList.clear();
				// send message to new nodes to create them with new id
				// send 10 in 'tag' property for detection of these messages
				freqList.clear();
				freqList.add(freq1);
				freqList.add(freq4);
				sendMessage(new LongWritable(this.vertex.getId().get() + 200000000), 
						new DBGProcessMessage(node1 + node3 + node4, (byte)10, len1, len4, freqList));
				
				freqList.clear();
				freqList.add(freq1);
				freqList.add(freq5);
				sendMessage(new LongWritable(this.vertex.getId().get() + 300000000), 
						new DBGProcessMessage(node1 + node3 + node5, (byte)10, len1, len5, freqList));
				
				freqList.clear();
				freqList.add(freq2);
				freqList.add(freq4);
				sendMessage(new LongWritable(this.vertex.getId().get() + 400000000), 
						new DBGProcessMessage(node2 + node3 + node4, (byte)10, len2 , len4, freqList));
				
				freqList.clear();
				freqList.add(freq2);
				freqList.add(freq5);
				sendMessage(new LongWritable(this.vertex.getId().get() + 500000000), 
						new DBGProcessMessage(node2 + node3 + node5, (byte)10, len2 , len5, freqList));
				
				
				
//					if (this.vertex.getValue().getInputEdges().contains(msg.getSourceId())) {
//						st1 = msg.getKmer();
//					}
////					if (this.vertex.getEdges())
//					for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges()) {
//						if (msg.getSourceId() == edge.getTargetVertexId().get()) 
//							st4 = msg.getKmer();
//					}	
//					
			}
			
			else if ((inputEdgeNum == 2) && (outputEdgeNum == 2)) {
				
				// Vertices that have 2 input and 2 outputs, are in middle of a Branch
				// set 'tag' property of Branch middle node to 6 
//				this.vertex.getValue().setTag((byte) 6);
				/**
				 * Vertices that have 2 input and 2 outputs, are in middle of a Branch 
				 * So this vertex send message '1' or '2' to inputs and '4' or '5' to outputs as a rank
				 * we use 'rank' parameter as index in branch 		
				 * rank shows node situation in branch.
				 * rank = 1 or 2 for input nodes
				 * rank = 4 or 5 for output nodes
				 */
				int rank = 1;
				for (Long inputId : this.vertex.getValue().getInputEdges()) {
					sendMessage(new LongWritable(inputId), 
							new DBGProcessMessage(this.vertex.getId().get(), rank));
					rank++;
				}
				
				for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges()) {
					rank++;
					sendMessage(edge.getTargetVertexId(), 
							new DBGProcessMessage(this.vertex.getId().get(), rank));					
				}				
	
				aggregate("tipCountAgg", new IntWritable(1));		 		
			}
			
		}
		
		//if (super.getSuperstep() % 2 == 0)
		else { 
		
			for (DBGProcessMessage msg : messages) {
				if (msg.getTag() != (byte)10) {	
					// set 'Rank' property as rank of nodes in branch
					// rank is node situation in branch.
					// rank = 1 or 2 for input nodes
					// rank = 4 or 5 for output nodes
					this.vertex.getValue().setRank(msg.getRank());
					// set 'Tag' property to '5' to show node is in branch
					// for using 'pred' and 'rank' property as branch parameters 
	//				this.vertex.getValue().setTag((byte) 5); 
					
					sendMessage(new LongWritable(msg.getSourceId()), 
							new DBGProcessMessage(this.vertex.getId().get(), 
									this.vertex.getValue().getKmer(), this.vertex.getValue().getFreq(), this.vertex.getValue().getRank()));
				}
				// tag=10 for new contigs after branch detection
				else if (msg.getTag() == (byte)10) {
					this.vertex.getValue().setKmer(msg.getKmer());
					this.vertex.getValue().setTag(msg.getTag());
					this.vertex.getValue().setPred(msg.getPred());
					this.vertex.getValue().setRank(msg.getRank());
					/**
					 keep freq of first and last node of branch for use in branch solving
					*/
					for(long frq : msg.getInputEdges()) {
						this.vertex.getValue().addNodeToInputEdges(frq);
					}
				}

			}
			
						
		}
		
	}


}
