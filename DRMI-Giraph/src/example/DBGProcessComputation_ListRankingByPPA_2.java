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
public class DBGProcessComputation_ListRankingByPPA_2 extends BasicComputation<LongWritable, 
DBGProcessVertexValue, NullWritable, DBGProcessMessage> {

	/**
	 * The current vertex
	 */
	private Vertex<LongWritable, DBGProcessVertexValue, NullWritable> vertex;
	
	/**
	 * The broadcast variable that show the start superstep of ListRanking2
	 */
	LongWritable rank2_superstep;
	
	@Override
	public void compute(Vertex<LongWritable, DBGProcessVertexValue, NullWritable> vertex, 
			Iterable<DBGProcessMessage> messages) throws IOException {		
		
		this.vertex = vertex;
		int outputEdgeNum = this.vertex.getNumEdges();
		int inputEdgeNum = this.vertex.getValue().getNumInputEdges();		
		this.rank2_superstep = getBroadcast("brdVar_rank2")	;  
		boolean odd_startRank2Superstep = (this.rank2_superstep.get() % 2 == 1);
		
		if (super.getSuperstep() == rank2_superstep.get()) {
			
			this.vertex.getValue().setPred(-20);
			this.vertex.getValue().setRank(-10);
			this.vertex.getValue().setTag((byte) 0);
									
			// Complex nodes send message to successor nodes
			if (outputEdgeNum > 1){
//				System.out.println("=======Complex Node Before Head of path=========");
				for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges()) {					
					sendMessage(edge.getTargetVertexId(), new DBGProcessMessage(this.vertex.getId().get(), "outComplex", this.vertex.getValue().getFreq(), this.vertex.getValue().getInputEdges()));
				}
			}
			
			// Complex nodes send message to predecessor nodes
			if (inputEdgeNum > 1){
//				System.out.println("=======Complex Node After Tail of path=========");
				for (long input : this.vertex.getValue().getInputEdges()) {					
					sendMessage(new LongWritable(input), new DBGProcessMessage(this.vertex.getId().get(), "inComplex", this.vertex.getValue().getFreq(), this.vertex.getValue().getInputEdges()));
				}
			}
						
		}		
		
		else if (super.getSuperstep() == rank2_superstep.get()+1) {
			
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
								
//				long input1 = this.vertex.getValue().getInputEdges().get(0);

				
				//Nodes before a complex node that have one output, receive a msg and set tag to Tail
				boolean beforeInComplex = false;
				if (outputEdgeNum == 1) {
					for(DBGProcessMessage msg: messages) {
						String strIn = "inComplex";
						if (strIn.compareTo(msg.getKmer()) == 0) {
							if ((inputEdgeNum == 1)) {
								long input1 = this.vertex.getValue().getInputEdges().get(0);
								this.vertex.getValue().setTag((byte) 2); //set tag to Tail
								this.vertex.getValue().setPred(input1);
								this.vertex.getValue().setRank(1);												
								
							}
							else {
								beforeInComplex = true;
								// نودی که از نود پیچیده بعدی خود پیام گرفت در صورتی که یک ورودی نباشد کاندید تیل نیست و هالت می شود.
//								this.vertex.voteToHalt(); 
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
								this.vertex.getValue().setTag((byte) 1); //set tag to Head
								this.vertex.getValue().setPred(-1);
								this.vertex.getValue().setRank(0);															
								
								for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges()) {
									sendMessage(edge.getTargetVertexId(), new DBGProcessMessage(this.vertex.getId().get(), this.vertex.getValue().getTag(), this.vertex.getValue().getPred(), this.vertex.getValue().getRank()));
								}
							}
							else {
								afterOutComplex = true;
								// نودی که از نود پیچیده قبلی خود پیام گرفت در صورتی که یک خروجی نباشد کاندید هد نیست و هالت می شود.
//								this.vertex.voteToHalt();
								
							}
		
						}
							
					}
				}
											
				// Head nodes send message with its tag to successor node - Head nodes that are not before a complex node
				if ((isHead) && (! beforeInComplex)){		
//					System.out.println("========Head of path========");
					this.vertex.getValue().setTag((byte) 1); // set Tag to Head
					this.vertex.getValue().setPred(-1);
					this.vertex.getValue().setRank(0);
									
					for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges()) {
						sendMessage(edge.getTargetVertexId(), new DBGProcessMessage(this.vertex.getId().get(), this.vertex.getValue().getTag(), this.vertex.getValue().getPred(), this.vertex.getValue().getRank()));
					}
					
				}					
				
				// Tail nodes set its' tag to Tail - Tail nodes that are not after complex node
				if ((isTail) && (! afterOutComplex)) {
//					System.out.println("========Tail of path========");
					this.vertex.getValue().setTag((byte) 2); // set Tag to Tail	
//					this.vertex.voteToHalt();// Tail nodes should be halted // مورخ 99/01/15 این خط کامنت شد چون نود آخر ممکن بود به خاطر هالت بودن در نظر گرفته نشود و عملیات رنکینگ زودتر از موعد تمام شود واین نود آخر به طور کامل رنک نشده باشد
				}
													
				// 1-1 nodes and Tail nodes set their pred and rank 
				if ((is1_1Node || isTail) && (! afterOutComplex) && (this.vertex.getValue().getTag() != (byte) 1)) {
					for (long input : this.vertex.getValue().getInputEdges()) {
						this.vertex.getValue().setPred(input);
						this.vertex.getValue().setRank(1);
						
					}	
				}	
				  
				/// با توجه به نیاز نودهای پیچیده در مرج بعد از بابل از هالت شدن این نودها جلوگیری شد
				// m-n nodes and some of 1-m and n-1 should be halted
//				if ((is1_1Node == false) && (isHead == false) && (isTail == false))
//					this.vertex.voteToHalt();
								
			}
//			else // if the vertex get more than 1 message, should not process them 
//				this.vertex.voteToHalt();
						
		}
	    
		else if (super.getSuperstep() == rank2_superstep.get()+2) {
						
			// Receive Message from Head by the first node after the Head
			if (inputEdgeNum == 1) {
				for(DBGProcessMessage msg: messages) {					
					this.vertex.getValue().setPred(msg.getPred());
					this.vertex.getValue().setRank(this.vertex.getValue().getRank() + msg.getRank());
				}
			}
			
			/// این شرط با توجه به امکان نیاز به نودها در حذف تیپ و بابل فعلا بلااستفاده می شود
//			else if (this.vertex.getValue().getTag() != (byte) 1)// nodes after Head that have more than one input edges and they are not Head
//				this.vertex.voteToHalt();
			
			// 1-1 nodes send its Pred and Rank to successor
			/// !!! این نودها می توانند نودهای ۱-۱ درگیر در بابل باشند 
			/// !!! که در این حالت پیام اضافی ارسال می شود			
			if ((outputEdgeNum == 1) && (inputEdgeNum == 1) && (this.vertex.getValue().getTag() != (byte) 2)) {
				for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges()) {
					sendMessage(edge.getTargetVertexId(), new DBGProcessMessage(this.vertex.getId().get(), this.vertex.getValue().getTag(), this.vertex.getValue().getPred(), this.vertex.getValue().getRank()));
				}				
			}
			
//			MergeAggregator agg1 = new MergeAggregator();
			if (this.vertex.getValue().getPred() == -1) // Condition for end of the List Ranking
				aggregate(BooleanAndAggregator.class.getName(), new BooleanWritable(true));
			else if (this.vertex.getValue().getRank() > 0) // if node is in simple path(used in ranking) and is not ranked(pred!=-1)
				aggregate(BooleanAndAggregator.class.getName(), new BooleanWritable(false));
		}
		
		
		else if ((super.getSuperstep() > rank2_superstep.get()+2) && (((odd_startRank2Superstep) && (super.getSuperstep() % 2 == 0)) || ((!odd_startRank2Superstep) && (super.getSuperstep() % 2 == 1)))) {			
					// 
			if (inputEdgeNum == 1) {
				for(DBGProcessMessage msg: messages) {
					
					///  نودهای ۱-۱ درگیر در لوپ ایزوله تگ ۸ میگیرند تا در مراحل بعدی کنترل شوند
					/// و از تکرار بیهوده عملیات جلوگیری شود
					/// ضمنا این نودها هالت می شوند
//					if ((this.vertex.getId().get() == msg.getPred())) {
//						this.vertex.getValue().setTag((byte) 8);
//						this.vertex.voteToHalt();
//					}
					
					this.vertex.getValue().setPred(msg.getPred());
					this.vertex.getValue().setRank(this.vertex.getValue().getRank() + msg.getRank());
			
					if (this.vertex.getValue().getPred() != -1) { // if node is not already ranked  
//						if (this.vertex.getValue().getTag() != (byte) 8) // if node is not in a isolated loop							
						sendMessage(new LongWritable(this.vertex.getValue().getPred()), new DBGProcessMessage(this.vertex.getId().get()));
//						System.out.println(this.vertex.getId().get() + "\t" + this.vertex.getValue().getKmer() + ";" +  this.vertex.getValue().getFreq() + ";" + this.vertex.getValue().getInputEdges() + "\t" +this.vertex.getEdges().);
						///TEST
//						if (super.getSuperstep() == 41) {
//							System.out.println("in ListRanking2");
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
			if (this.vertex.getValue().getPred() == -1) // Condition for end of the List Ranking
				aggregate(BooleanAndAggregator.class.getName(), new BooleanWritable(true));
			else if (this.vertex.getValue().getRank() > 0) // if node is in simple path(used in ranking) and is not ranked(pred!=-1)
//				if (this.vertex.getValue().getTag() != (byte) 8) // if node is not in a isolated loop
				aggregate(BooleanAndAggregator.class.getName(), new BooleanWritable(false));
			
		}
		
		else if ((super.getSuperstep() > rank2_superstep.get()+2) && (((odd_startRank2Superstep) && (super.getSuperstep() % 2 == 1)) || ((!odd_startRank2Superstep) && (super.getSuperstep() % 2 == 0)))) {
			for(DBGProcessMessage msg: messages) {													
				sendMessage(new LongWritable(msg.getSourceId()), new DBGProcessMessage(this.vertex.getId().get(), this.vertex.getValue().getTag(), this.vertex.getValue().getPred(), this.vertex.getValue().getRank()));
			}
			
			//Set Aggregator
			if (this.vertex.getValue().getPred() == -1) // Condition for end of the List Ranking
				aggregate(BooleanAndAggregator.class.getName(), new BooleanWritable(true));
			else if (this.vertex.getValue().getRank() > 0) // if node is in simple path(used in ranking) and is not ranked(pred!=-1)
//				if (this.vertex.getValue().getTag() != (byte) 8) // if node is not in a isolated loop
				aggregate(BooleanAndAggregator.class.getName(), new BooleanWritable(false));
		}
					
//		System.out.println("######################");
//		System.out.println("*********SUPERSTEP: " + super.getSuperstep());
//		System.out.println("*********Vertex Id: " + this.vertex.getId().get());
//		System.out.println("*********Vertex Rank: " + this.vertex.getValue().getRank());
//		System.out.println("*********Vertex Tag: " + this.vertex.getValue().getTag());
//		System.out.println("*********Vertex pred: " + this.vertex.getValue().getPred());
//		System.out.println("*********Vertex is Halt: " + this.vertex.isHalted());
//		System.out.println("######################");
			
		
	}
	
}