package example;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * Calculates how many hops there are between different vertices in the graph.
 * Hops could be also seen as the degree of separation.
 */
public class DBGProcessComputation_ListRankingByPPA extends BasicComputation<LongWritable, 
DBGProcessVertexValue, NullWritable, DBGProcessMessage> {

	/**
	 * The current vertex
	 */
//	private Vertex<LongWritable, DBGProcessVertexValue, NullWritable> vertex;
	
//	/**
//	 * The Flag for halt
//	 */
//	private boolean haltFlag = false;
	
	@Override
	public void compute(Vertex<LongWritable, DBGProcessVertexValue, NullWritable> vertex, 
			Iterable<DBGProcessMessage> messages) throws IOException {		
		
//		LongWritable kmerSize = getBroadcast("brdVar_kmerSize");
//		System.out.println("---> broadcasted kmerSize is : " + kmerSize);
		
//		vertex = vertex;
		int outputEdgeNum = vertex.getNumEdges();
		int inputEdgeNum = vertex.getValue().getNumInputEdges();		
			  
		
		if (super.getSuperstep() == 0) {
			if (outputEdgeNum > 0) {
				for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
					sendMessage(edge.getTargetVertexId(), new DBGProcessMessage(vertex.getId().get()));					
				}
			}
		}
		
		if (super.getSuperstep() == 1) {
			
			for(DBGProcessMessage msg: messages) {
				vertex.getValue().addNodeToInputEdges(msg.getSourceId());
			}
			
		}
		
		if (super.getSuperstep() == 2) {
			
			// Complex nodes send message to successor nodes
			if (outputEdgeNum > 1){
//				System.out.println("=======Complex Node Before Head of path=========");
				for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {					
					sendMessage(edge.getTargetVertexId(), new DBGProcessMessage(vertex.getId().get(), "outComplex", vertex.getValue().getFreq(), vertex.getValue().getInputEdges()));
				}
			}
			
			// Complex nodes send message to predecessor nodes
			if (inputEdgeNum > 1){
//				System.out.println("=======Complex Node After Tail of path=========");
				for (long input : vertex.getValue().getInputEdges()) {					
					sendMessage(new LongWritable(input), new DBGProcessMessage(vertex.getId().get(), "inComplex", vertex.getValue().getFreq(), vertex.getValue().getInputEdges()));
				}
			}
						
		}		
		
		else if (super.getSuperstep() == 3) {
			
			// Count messages received by this vertex
			int msgCnt = 0;
			for (DBGProcessMessage cmsg : messages) {
				msgCnt++ ;				
			}
			
			
			boolean isHead = ((outputEdgeNum == 1) && (inputEdgeNum > 1 || inputEdgeNum == 0));
			boolean isTail = ((inputEdgeNum == 1) && (outputEdgeNum > 1 || outputEdgeNum == 0));
			boolean is1_1Node = ((outputEdgeNum == 1) && (inputEdgeNum == 1));					
			// if the vertex get more than 1 message, should not process them 
			if ((msgCnt <= 1) || isHead || isTail) {
								
//				long input1 = vertex.getValue().getInputEdges().get(0);

				
				//Nodes before a complex node that have one output, receive a msg and set tag to Tail
				boolean beforeInComplex = false;
				if (outputEdgeNum == 1) {
					for(DBGProcessMessage msg: messages) {
						String strIn = "inComplex";
						if (strIn.compareTo(msg.getKmer()) == 0) {
							if ((inputEdgeNum == 1)) {
								long input1 = vertex.getValue().getInputEdges().get(0);
								vertex.getValue().setTag((byte) 2); //set tag to Tail
								vertex.getValue().setPred(input1);
								vertex.getValue().setRank(1);												
								
							}
							else {
								beforeInComplex = true;
								// نودی که از نود پیچیده بعدی خود پیام گرفت در صورتی که یک ورودی نباشد کاندید تیل نیست و هالت می شود.
//								vertex.voteToHalt(); 
							}
						}
							
					}
				}
				
				//Nodes after a complex node that have one input, receive a msg and set tag to Head
				boolean afterOutComplex = false;
				if (inputEdgeNum == 1) {
					for(DBGProcessMessage msg: messages) {
						String strOut = "outComplex";
						if (strOut.compareTo(msg.getKmer()) == 0) {
							if ((outputEdgeNum == 1)) {
								vertex.getValue().setTag((byte) 1); //set tag to Head
								vertex.getValue().setPred(-1);
								vertex.getValue().setRank(0);															
								
								for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
									sendMessage(edge.getTargetVertexId(), new DBGProcessMessage(vertex.getId().get(), vertex.getValue().getTag(), vertex.getValue().getPred(), vertex.getValue().getRank()));
								}
							}
							else {
								afterOutComplex = true;
								// نودی که از نود پیچیده قبلی خود پیام گرفت در صورتی که یک خروجی نباشد کاندید هد نیست و هالت می شود.
//								vertex.voteToHalt();
								
							}
		
						}
							
					}
				}
											
				// Head nodes send message with its tag to successor node - Head nodes that are not before a complex node
				if ((isHead) && (! beforeInComplex)){		
//					System.out.println("========Head of path========");
					vertex.getValue().setTag((byte) 1); // set Tag to Head
					vertex.getValue().setPred(-1);
					vertex.getValue().setRank(0);
									
					for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
						sendMessage(edge.getTargetVertexId(), new DBGProcessMessage(vertex.getId().get(), vertex.getValue().getTag(), vertex.getValue().getPred(), vertex.getValue().getRank()));
					}
					
				}					
				
				// Tail nodes set its' tag to Tail - Tail nodes that are not after complex node
				if ((isTail) && (! afterOutComplex)) {
//					System.out.println("========Tail of path========");
					vertex.getValue().setTag((byte) 2); // set Tag to Tail	
					vertex.voteToHalt();// Tail nodes should be halted
				}
													
				// 1-1 nodes and Tail nodes set their pred and rank 
				if ((is1_1Node || isTail) && (! afterOutComplex) && (vertex.getValue().getTag() != (byte) 1)) {
					for (long input : vertex.getValue().getInputEdges()) {
						vertex.getValue().setPred(input);
						vertex.getValue().setRank(1);
						
					}	
				}	
				  
				/// با توجه به نیاز نودهای پیچیده در مرج بعد از بابل از هالت شدن این نودها جلوگیری شد
				// m-n nodes and some of 1-m and n-1 should be halted
//				if ((is1_1Node == false) && (isHead == false) && (isTail == false))
//					vertex.voteToHalt();
								
			}
//			else // if the vertex get more than 1 message, should not process them 
//				vertex.voteToHalt();
						
		}
	    
		else if (super.getSuperstep() == 4) {
						
			// Receive Message from Head by the first node after the Head
			if (inputEdgeNum == 1) {
				for(DBGProcessMessage msg: messages) {					
					vertex.getValue().setPred(msg.getPred());
					vertex.getValue().setRank(vertex.getValue().getRank() + msg.getRank());
				}
			}
			
			/// این شرط با توجه به امکان نیاز به نودها در حذف تیپ و بابل فعلا بلااستفاده می شود
//			else if (vertex.getValue().getTag() != (byte) 1)// nodes after Head that have more than one input edges and they are not Head
//				vertex.voteToHalt();
			
			// 1-1 nodes send its Pred and Rank to successor
			/// !!! این نودها می توانند نودهای ۱-۱ درگیر در بابل باشند 
			/// !!! که در این حالت پیام اضافی ارسال می شود			
			if ((outputEdgeNum == 1) && (inputEdgeNum == 1) && (vertex.getValue().getTag() != (byte) 2)) {
				for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
					sendMessage(edge.getTargetVertexId(), new DBGProcessMessage(vertex.getId().get(), vertex.getValue().getTag(), vertex.getValue().getPred(), vertex.getValue().getRank()));
				}				
			}
			
//			MergeAggregator agg1 = new MergeAggregator();
			if (vertex.getValue().getPred() == -1) // Condition for end of the List Ranking
				aggregate(BooleanAndAggregator.class.getName(), new BooleanWritable(true));
			else if (vertex.getValue().getRank() > 0) // if node is in simple path(used in ranking) and is not ranked(pred!=-1)
				aggregate(BooleanAndAggregator.class.getName(), new BooleanWritable(false));
		}
		
		
		else if ((super.getSuperstep() > 4) && (super.getSuperstep() % 2 == 1)) {
			
			// 
			if (inputEdgeNum == 1) {
				for(DBGProcessMessage msg: messages) {
					
					///  نودهای ۱-۱ درگیر در لوپ ایزوله تگ ۸ میگیرند تا در مراحل بعدی کنترل شوند
					/// و از تکرار بیهوده عملیات جلوگیری شود
					/// ضمنا این نودها هالت می شوند
//					if ((vertex.getId().get() == msg.getPred())) {
//						vertex.getValue().setTag((byte) 8);
//						vertex.voteToHalt();
//					}
					
					vertex.getValue().setPred(msg.getPred());
					vertex.getValue().setRank(vertex.getValue().getRank() + msg.getRank());
			
					if (vertex.getValue().getPred() != -1) { // if node is not already ranked  
//						if (vertex.getValue().getTag() != (byte) 8) // if node is not in a isolated loop							
						sendMessage(new LongWritable(vertex.getValue().getPred()), new DBGProcessMessage(vertex.getId().get()));
//						System.out.println(vertex.getId().get() + "\t" + vertex.getValue().getKmer() + ";" +  vertex.getValue().getFreq() + ";" + vertex.getValue().getInputEdges() + "\t" +vertex.getEdges().);
						///TEST
//						if (super.getSuperstep() == 41) {
//							System.out.println("in ListRanking");
//							System.out.println("Number of Vertices: " + getTotalNumVertices());
//							StringBuilder sb = new StringBuilder();
//							
//							sb.append(vertex.getId());
//							sb.append('\t');
//							sb.append(vertex.getValue().getKmer());
//							sb.append(';');
//							sb.append(vertex.getValue().getFreq());
//							sb.append(';');
//							sb.append(vertex.getValue().getInputEdges());
//							sb.append('\t');
//							
//							sb.append(vertex.getValue().getPred());
//							sb.append(';');
//							sb.append(vertex.getValue().getRank());
//							sb.append(';');
//							sb.append(vertex.getValue().getTag());
//							sb.append('\t');
//							for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {	    	
//								sb.append(edge.getTargetVertexId());
//								sb.append(",");	        
//							}
//  
//							System.out.println(sb);
//						}
						///TEST
					}
				}				
			}
			
//			MergeAggregator agg1 = new MergeAggregator();
			if (vertex.getValue().getPred() == -1) // Condition for end of the List Ranking
				aggregate(BooleanAndAggregator.class.getName(), new BooleanWritable(true));
			else if (vertex.getValue().getRank() > 0) // if node is in simple path(used in ranking) and is not ranked(pred!=-1)
//				if (vertex.getValue().getTag() != (byte) 8) // if node is not in a isolated loop
				aggregate(BooleanAndAggregator.class.getName(), new BooleanWritable(false));
			
		}
		
		else if ((super.getSuperstep() > 4) && (super.getSuperstep() % 2 == 0)) {			
			for(DBGProcessMessage msg: messages) {													
				sendMessage(new LongWritable(msg.getSourceId()), new DBGProcessMessage(vertex.getId().get(), vertex.getValue().getTag(), vertex.getValue().getPred(), vertex.getValue().getRank()));
			}
			
			//Set Aggregator
			if (vertex.getValue().getPred() == -1) // Condition for end of the List Ranking
				aggregate(BooleanAndAggregator.class.getName(), new BooleanWritable(true));
			else if (vertex.getValue().getRank() > 0) // if node is in simple path(used in ranking) and is not ranked(pred!=-1)
//				if (vertex.getValue().getTag() != (byte) 8) // if node is not in a isolated loop
				aggregate(BooleanAndAggregator.class.getName(), new BooleanWritable(false));
		}
					
//		System.out.println("######################");
//		System.out.println("*********SUPERSTEP: " + super.getSuperstep());
//		System.out.println("*********Vertex Id: " + vertex.getId().get());
//		System.out.println("*********Vertex Rank: " + vertex.getValue().getRank());
//		System.out.println("*********Vertex Tag: " + vertex.getValue().getTag());
//		System.out.println("*********Vertex pred: " + vertex.getValue().getPred());
//		System.out.println("*********Vertex is Halt: " + vertex.isHalted());
//		System.out.println("######################");
//		messages = null;		
		
	}
	
}