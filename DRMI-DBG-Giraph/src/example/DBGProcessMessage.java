package example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Message-Type used by {@link org.apache.giraph.examples
 * .SimpleHopsComputation} for communication.
 */
public class DBGProcessMessage implements Writable {
		
	/**
	  * the id of the vertex initiating this message.
	 */
	private long sourceId;
	
	/**
	 * the id of the vertex of the final destination of this message.
	 */
	private long destinationId;
			
	/**
	 * the Kmer of sender.
	 */
	private String strKmer = "";
	
	/**
	 * the Freq of sender.
	 */
	private int intFreq = 0;
			
	/**
	 * the input edge list of sender.
	 */
	private List<Long> inputEdgesList = new ArrayList<Long>();
	
		
	//parameters for list ranking messages
	
	/**
	 * vertex tag {1 for Head, 2 for Tail, 3 for Complex nodes, 0 for other nodes}
	 */
	private byte nodeTag;
	
	/**
	 * the Id of predecessor node to get the Head of a simple path
	 */
	private long nodePred;
	
	/**
	 * Rank of node in simple path
	 */
	private int nodeRank;
	
	
	
	/**
	 * Default constructor for reflection
	 */
	public DBGProcessMessage() {
	}
	
	/**
	 * constructor used by DBGProcessComputation
	 */
	public DBGProcessMessage(long sourceId, String kmer, int freq, List<Long> neighborsList) {
		this.sourceId = sourceId;
		this.strKmer = kmer;
		this.intFreq = freq;
		this.inputEdgesList = neighborsList;		
	}
	
	
	/**
	 * constructor used in list ranking
	 */
	public DBGProcessMessage(long sourceId, byte tag, long pred, int rank) {
		this.sourceId = sourceId;
		this.nodeTag = tag;
		this.nodePred = pred;
		this.nodeRank = rank;		
	}

	/**
	 * constructor used for set inputEdges --> message from a node to its suc 
	 * constructor used in list ranking --> message from a node to its pred
	 */
	public DBGProcessMessage(long sourceId) {
		this.sourceId = sourceId;			
	}	

	/**
	 * constructor used in Merge
	 */
	public DBGProcessMessage(long sourceId, String kmer, int freq, List<Long> neighborsList, long pred, int rank) {
		this.sourceId = sourceId;
		this.strKmer = kmer;
		this.intFreq = freq;
		this.inputEdgesList = neighborsList;	
		this.nodePred = pred;
		this.nodeRank = rank;		
	}
	
	/**
	 * constructor used in Branch detecttion
	 * we use 'rank' parameter for detecting node situation.
	 * rank = 1 or 2 for input nodes
	 * rank = 3 or 4 for output nodes
	 */
	public DBGProcessMessage(long sourceId, int rank) {
		this.sourceId = sourceId;
		this.nodeRank = rank;		
	}
	
	/**
	 * constructor used in Branch Solving
	 * 
	 */
	public DBGProcessMessage(long sourceId, String kmer, int freq, int rank) {
		this.sourceId = sourceId;
		this.strKmer = kmer;
		this.intFreq = freq;
		this.nodeRank = rank;
	}
	
	/**
	 * constructor used in Branch Solving
	 * 
	 */
	public DBGProcessMessage(String kmer, byte tag, long pred, int rank, List<Long> neighborsList) {
		this.strKmer = kmer;
		this.nodeTag = tag;
		this.nodePred = pred;
		this.nodeRank = rank;
		this.inputEdgesList = neighborsList; // use for save freq of branch nodes
	}
	
	public long getSourceId() {
		
		return this.sourceId;		
	}

	public long getDestinationId() {
		
		return this.destinationId;		
	}

	public String getKmer() {
		
		return this.strKmer;		
	}
	
	public int getFreq() {
		
		return this.intFreq;		
	}
	
	public List<Long> getInputEdges() {
		
		return this.inputEdgesList;		
	}
	
	
	
	/**
	 * 
	 */
	public byte getTag() {
		return this.nodeTag;
	}
	
	public void setTag(byte tag) {
		this.nodeTag = tag;
	}
	
	/**
	 * 
	 */
	public long getPred() {
		return this.nodePred;
	}
	
	public void setPred(long pred) {
		this.nodePred = pred;
	}
	
	/**
	 * 
	 */
	public int getRank() {
		return this.nodeRank;
	}
	
	public void setRank(int rank) {
		this.nodeRank = rank;
	}
	
	
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		// TODO Auto-generated method stub
		this.sourceId = dataInput.readLong();
	    this.destinationId = dataInput.readLong();
	    
	    Text tmpTxt = new Text();
		tmpTxt.readFields(dataInput);
		this.strKmer = tmpTxt.toString();
		
		this.intFreq = dataInput.readInt();
		
		int size = dataInput.readInt();
		
		this.inputEdgesList.clear();//list should be cleared because may be append to to previous value
		for(int i=0; i < size; i++) {
			this.inputEdgesList.add(dataInput.readLong());
		}				
		
		this.nodeTag = dataInput.readByte();
		this.nodePred = dataInput.readLong();
		this.nodeRank = dataInput.readInt();
					   
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		// TODO Auto-generated method stub
		dataOutput.writeLong(this.sourceId);
	    dataOutput.writeLong(this.destinationId);
	    
	    Text tmpTxt = new Text();
	    tmpTxt.set(this.strKmer);
	    tmpTxt.write(dataOutput);	    
//		dataOutput.writeChars(this.strKmer);
		
		dataOutput.writeInt(this.intFreq);
		int size = this.inputEdgesList.size();
		dataOutput.writeInt(size);
		for(int i=0; i < size; i++) {
			dataOutput.writeLong(this.inputEdgesList.get(i));
		}
				
		dataOutput.write(this.nodeTag);
		dataOutput.writeLong(this.nodePred);
		dataOutput.writeInt(this.nodeRank);
			    
	}

}