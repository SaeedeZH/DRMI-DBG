package example;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
//import org.weakref.jmx.com.google.common.collect.Iterables;

import java.io.IOException;

/**
 * Calculates how many hops there are between different vertices in the graph.
 * Hops could be also seen as the degree of separation.
 */
public class DBGProcessComputation_Merge extends BasicComputation<LongWritable, 
DBGProcessVertexValue, NullWritable, DBGProcessMessage> {

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
//		System.out.println("---> broadcasted kmerSize is : " + this.kmerSize);
		
		///Test
//		if ((getSuperstep() == 65) || (getSuperstep() == 66)) {
//			if ((this.vertex.getId().get() == 163276) || (this.vertex.getId().get() == 138) || (this.vertex.getId().get() == 105492) || (this.vertex.getId().get() == 194650)) {
//				System.out.println("in merge");
//				System.out.println("Number of Vertices: " + getTotalNumVertices());
//				StringBuilder sb = new StringBuilder();
//				
//				sb.append(vertex.getId());
//				sb.append('\t');
//				sb.append(vertex.getValue().getKmer());
//				sb.append(';');
//				sb.append(vertex.getValue().getFreq());
//				sb.append(';');
//				sb.append(vertex.getValue().getInputEdges());
//				sb.append('\t');
//				
//				sb.append(vertex.getValue().getPred());
//				sb.append(';');
//				sb.append(vertex.getValue().getRank());
//				sb.append(';');
//				sb.append(vertex.getValue().getTag());
//				sb.append('\t');
//				for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges()) {
//					sb.append(edge.getTargetVertexId());
//					sb.append(",");
//				}
//		
//				System.out.println(sb);
//			}
//		
//		}
		///Test
		
//		int outputEdgeNum = this.vertex.getNumEdges();
//		int inputEdgeNum = this.vertex.getValue().getNumInputEdges();
		
//		if (getSuperstep() == 20) {
//			System.out.println("Number of Vertices: " + getTotalNumVertices());
////			System.out.println(this.vertex.getId().get() + "\t" + this.vertex.getValue().getKmer() + ";" +  this.vertex.getValue().getFreq() + ";" + this.vertex.getValue().getInputEdges() + "\t" +this.vertex.getEdges().);
//			StringBuilder sb = new StringBuilder();
//			
//		      sb.append(vertex.getId());
//		      sb.append('\t');
//		      sb.append(vertex.getValue().getKmer());
//		      sb.append(';');
//		      sb.append(vertex.getValue().getFreq());
//		      sb.append(';');
//		      sb.append(vertex.getValue().getInputEdges());
//		      sb.append('\t');
//		      
//		      sb.append(vertex.getValue().getPred());
//		      sb.append(';');
//		      sb.append(vertex.getValue().getRank());
//		      sb.append(';');
//		      sb.append(vertex.getValue().getTag());
//		      sb.append('\t');
//		     
//		      for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {	    	
//		    	  sb.append(edge.getTargetVertexId());
//		    	  sb.append(",");	        
//		      }
//		      
//		      System.out.println(sb);
//		}
	  
		if (super.getSuperstep() % 2 == 0) {
			
//			System.out.println("merge even super.getSuperstep(): "+ super.getSuperstep());
			
//			//new**************
//			for (DBGProcessMessage msg: messages) {
////				if (this.vertex.getValue().getInputEdges().contains(msg.getSourceId()) == false)
//				this.vertex.addEdge(EdgeFactory.create(new LongWritable(msg.getSourceId())));
//			}
//			//******************
			
			// if the node is ranked and its rank is even and the node is not tail
			if ((this.vertex.getValue().getPred() == -1) && (this.vertex.getValue().getRank() % 2 == 0) && (this.vertex.getValue().getTag() != (byte) 2)) {
				String kmerSubstring;
				for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges()) {
//					sendMessage(edge.getTargetVertexId(), new DBGProcessMessage(this.vertex.getId().get(), this.vertex.getValue().getKmer(), this.vertex.getValue().getFreq(), this.vertex.getValue().getInputEdges()));
					
//					System.out.println("this.kmerSize:" + this.kmerSize);
//					System.out.println("this.vertex.getValue().getKmer():" + this.vertex.getValue().getKmer());
//					System.out.println("(0, this.vertex.getValue().getKmer().length() - DBGProcessMasterCompute.kmerSize + 1): " + 0 + ", " + this.vertex.getValue().getKmer().length() + ", " + DBGProcessMasterCompute.kmerSize+1);
					
					kmerSubstring = this.vertex.getValue().getKmer().substring(0, this.vertex.getValue().getKmer().length() - (int) this.kmerSize.get() + 1);
					sendMessage(edge.getTargetVertexId(), new DBGProcessMessage(this.vertex.getId().get(), kmerSubstring, this.vertex.getValue().getFreq(), this.vertex.getValue().getInputEdges()));
					
					///TEST
//					if (super.getSuperstep() == 80) {
//						System.out.println("in Merge");
//						System.out.println("Number of Vertices: " + getTotalNumVertices());
//						StringBuilder sb = new StringBuilder();
//						
//						sb.append(vertex.getId());
//						sb.append('\t');
//						sb.append(vertex.getValue().getKmer());
//						sb.append(';');
//						sb.append(vertex.getValue().getFreq());
//						sb.append(';');
//						sb.append(vertex.getValue().getInputEdges());
//						sb.append('\t');
//						
//						sb.append(vertex.getValue().getPred());
//						sb.append(';');
//						sb.append(vertex.getValue().getRank());
//						sb.append(';');
//						sb.append(vertex.getValue().getTag());
//						sb.append('\t');
//						sb.append(edge.getTargetVertexId());
//						sb.append(",");	        							
//  
//						System.out.println(sb);
//					}
						///TEST
				}
			
				aggregate(BooleanAndAggregator.class.getName(), new BooleanWritable(false));
			}		
			
//			else if ((this.vertex.getValue().getTag() == (byte) 2) && (this.vertex.getValue().getRank() % 2 == 0))
//				this.vertex.getValue().setRank(this.vertex.getValue().getRank()/2);
			
			else if (this.vertex.getValue().getTag() == (byte) 2)
				aggregate(BooleanAndAggregator.class.getName(), new BooleanWritable(true));
//				this.vertex.voteToHalt();			
			
			if ((this.vertex.getValue().getTag() == (byte) 2) && (this.vertex.getValue().getRank() % 2 == 0))
				this.vertex.getValue().setRank(this.vertex.getValue().getRank()/2);
			
		}	    
		
		else if (super.getSuperstep() % 2 == 1) {

			///TEST
//			if (super.getSuperstep() == 65) {
//				for (DBGProcessMessage msg:messages) {
//					
//					System.out.println("msg.getSourceId():" + msg.getSourceId());
//				
//					System.out.println("in Merge");
//					System.out.println("Number of Vertices: " + getTotalNumVertices());
//					StringBuilder sb = new StringBuilder();
//					
//					sb.append(vertex.getId());
//					sb.append('\t');
//					sb.append(vertex.getValue().getKmer());
//					sb.append(';');
//					sb.append(vertex.getValue().getFreq());
//					sb.append(';');
//					sb.append(vertex.getValue().getInputEdges());
//					sb.append('\t');
//					
//					sb.append(vertex.getValue().getPred());
//					sb.append(';');
//					sb.append(vertex.getValue().getRank());
//					sb.append(';');
//					sb.append(vertex.getValue().getTag());
//					sb.append('\t');
//					for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges()) {
//						sb.append(edge.getTargetVertexId());
//						sb.append(",");
//					}
//	
//					System.out.println(sb);
//				}
//						
//			}///TEST
			
			if (this.vertex.getValue().getInputEdges().size() == 1) {
			  for(DBGProcessMessage msg: messages) {
				  String currentKmer = this.vertex.getValue().getKmer();
//				  int currentKmerSize = currentKmer.length();
				  // whole kmer recieved
//				  String newKmer = msg.getKmer().concat(currentKmer.substring(DBGProcessMasterCompute.kmerSize-1, currentKmerSize));
				  // first character of kmer recieved
				  String newKmer = msg.getKmer().concat(currentKmer);
				  this.vertex.getValue().setKmer(newKmer);	
				  
				  // add frequency of predecessor to current freq
				  this.vertex.getValue().setFreq((this.vertex.getValue().getFreq() + msg.getFreq())/2);
				  
				  this.vertex.getValue().delNodeFromInputEdges(msg.getSourceId());				  				  				
				  
				  // for loop on input nodes of message sender
				  for(long inputId : msg.getInputEdges()) {
					  			
					  // condition for prevent add a node to its output list
					  // شرط برای جلوگیری از اضافه شدن یک نود به لیست خروجی های خودش
					  // prevent self loop edge
					  if (inputId != this.vertex.getId().get()) {						  						  
						  
//						  LongWritable lngWrInputId = new LongWritable(inputId);
//						  Edge<LongWritable, NullWritable> inputEdge = EdgeFactory.create(lngWrInputId);						  
						  
						  
//						  if (Iterables.contains(this.vertex.getEdges(), inputEdge) == false)
//						  if (Arrays.asList(this.vertex.getEdges()).contains(inputEdge) == false)
							  						
						  boolean nodeIsinEdges = false;
						  for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges()) {
//							  
							 if (edge.getTargetVertexId().get() == inputId) {
								 nodeIsinEdges = true;
								 break;
							 }
	
						  }
						  
						  // Prevent tandem repeat...
						  if (! nodeIsinEdges) {
							  // add nodes to input list of current vertex
							  this.vertex.getValue().addNodeToInputEdges(inputId);
							  
//							  // Send message to inputId for adding current vetextId to its output list - instead of addEdgeRequest
//							  // for checking a node that not exist in input list while adding to edge(output list)   
//							  sendMessage(new LongWritable(inputId), new DBGProcessMessage(this.vertex.getId().get()));
							  
							  // add current vetextId to output list of input vertices  by request
							  addEdgeRequest(new LongWritable(inputId), EdgeFactory.create(this.vertex.getId()));
						  }
						 						  
					  }
						  					  
					  // remove deleted node from output list of input vertices 
					  removeEdgesRequest(new LongWritable(inputId), new LongWritable(msg.getSourceId()));
					  
					  
				  }
				  
				  removeVertexRequest(new LongWritable(msg.getSourceId()));
				  
				  this.vertex.getValue().setRank((this.vertex.getValue().getRank()-1)/2);				  
				  				  
			  }
			}
			
			
			if (this.vertex.getValue().getTag() == (byte) 2)
				aggregate(BooleanAndAggregator.class.getName(), new BooleanWritable(false));
		
		}
		
		
		//Test Halt
//		if (getSuperstep() >= 20)
//			this.vertex.voteToHalt();
		
		
		//Test Print
//		System.out.println("######################");
//		System.out.println("*********GetWorkerCount: " + super.getWorkerContext().getWorkerCount());
//		System.out.println("*********GetWorkerIndex: " + super.getWorkerContext().getMyWorkerIndex());
//	
//		System.out.println("*********SUPERSTEP: " + super.getSuperstep());
//		System.out.println("*********Vertex Id: " + this.vertex.getId().get());
//		System.out.println("*********Vertex Rank: " + this.vertex.getValue().getRank());
//		System.out.println("*********Vertex Tag: " + this.vertex.getValue().getTag());
//		System.out.println("*********Vertex pred: " + this.vertex.getValue().getPred());
//		System.out.println("*********Vertex is Halt: " + this.vertex.isHalted());
//		System.out.println("######################");
//		messages = null;
//		try {
//			finalize();
//		} catch (Throwable e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
	}
	
}