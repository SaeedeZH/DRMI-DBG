package example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


/**
 * Value-Type for vertices used by {@link org.apache.giraph.DBGProcessComputation2
 * .DBGProcessComputation}
 */
public class DBGProcessVertexValue implements Writable{
	
	/**
	 * Kmer of node
	 */
	private String strKmer ;
	
	/**
	 * Frequency of node 
	 */
	private int intFreq ;
	
	/**
	 * List of input nodes
	 */
	private List<Long> inputEdgesList = new ArrayList<Long>();
	
	/**
	 * vertex tag {1 for Head, 2 for Tail, 3 for Complex nodes, 0 for other nodes}.
	 * Default value is 0
	 */
	private byte nodeTag;
	
	/**
	 * the Id of predecessor node to get the Head of a simple path.
	 * Default value is -20
	 */
	private long nodePred = -20;
	
	/**
	 * Rank of node in simple path.
	 * Default value is -10 
	 */
	private int nodeRank = -10;
		
	
	public void initializeValueParams(String values) {
		
		this.strKmer = values.split(",")[0];
		this.intFreq = Integer.parseInt(values.split(",")[1]);
		
//		String strTmp = values.split(";")[2];
//		String[] strInputEdges = new String[0];
//		strTmp = strTmp.substring(1, strTmp.length()-1);
//		if (strTmp.length() > 0)
//			strInputEdges = strTmp.split(", ");
//		
////		this.inputEdgesList = Arrays.asList(strInputEdges);		
////		String[] strInputEdges = values.split(";")[2].split(",");
//		
//		for (int i=0; i<strInputEdges.length; i++) {
//			this.inputEdgesList.add(Long.parseLong(strInputEdges[i]));
//		}
		
	}
	
	/**
	 * 
	 */
	public String getKmer() {
		return this.strKmer;
	}
	
	public void setKmer(String kmer) {
		this.strKmer = kmer;
	}
	
	/**
	 * 
	 */
	public int getFreq() {
		return this.intFreq;
	}

	public void setFreq(int freq) {
		this.intFreq = freq;
	}

	/**
	 * 
	 */
	public List<Long> getInputEdges() {
		return this.inputEdgesList;
	}

	public void addNodeToInputEdges(Long nodeId) {
		this.inputEdgesList.add(nodeId);		
	}
	
	/**
	 * 
	 */
	public void delNodeFromInputEdges(Long nodeId) {
		this.inputEdgesList.remove(this.inputEdgesList.indexOf(nodeId));
	}
	
	public int getNumInputEdges() {
		return this.inputEdgesList.size();
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
		
		Text tmpTxt = new Text();
		tmpTxt.readFields(dataInput);
		this.strKmer = tmpTxt.toString();
		
		this.intFreq = dataInput.readInt();
		int size = dataInput.readInt();
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
