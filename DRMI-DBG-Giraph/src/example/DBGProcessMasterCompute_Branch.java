package example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.giraph.counters.GiraphStats;

import org.apache.giraph.conf.LongConfOption;

public class DBGProcessMasterCompute_Branch extends DefaultMasterCompute{

	
//	String mergeAgg = "boolAgg";
	
//	MergeAggregator agg1 = new MergeAggregator();
	
	/**
	 * The (k-1)mer size 
	 */
	private int kmerSize;
	
	public static final LongConfOption k_value =
	      new LongConfOption("k_value", 0,
	          "The value of kmerSize");
	
	
	private boolean mergeFlag = false;
	private boolean tipFlag = false;
	private boolean bubFlag = false;
	private boolean rank2Flag = false;
	private boolean merge2Flag = false;
	private boolean branchFlag = false;
	private int superStep = 0;
	
	private String tipCountAgg = "tipCountAgg";
	
	
	private long msgcnt = -1;
	private boolean msgCntFlag = false;
	
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		// Initialization phase, used to initialize aggregator/Reduce/Broadcast
		// or to initialize other objects
		
		kmerSize = (int) k_value.get(getConf());
		System.out.println("---> kmerSize is : " + kmerSize);
		
//		broadcast("brdVar_kmerSize", new LongWritable(kmerSize));
		
		try {
//			registerAggregator(MergeAggregator.class.getName(), MergeAggregator.class);
//			registerAggregator(mergeAgg, MergeAggregator.class);
			registerAggregator(BooleanAndAggregator.class.getName(), BooleanAndAggregator.class);
			registerAggregator(tipCountAgg, IntSumAggregator.class);
		}
		catch (Exception e) {
			// TODO: handle exception
			System.out.println("*********registerAgg Error: " + e.toString());
		}
	}
	@Override
	public void compute() {
		// MasterCompute body
		
		
//		boolean workerAgg = ((BooleanWritable) getAggregatedValue(mergeAgg)).get();		
//		boolean workerAgg = ((BooleanWritable) getAggregatedValue(MergeAggregator.class.getName())).get();
		boolean workerAgg = ((BooleanWritable) getAggregatedValue(BooleanAndAggregator.class.getName())).get();
		
		GiraphStats gstat = GiraphStats.getInstance();
		System.out.println("#####################################################");		
		System.out.println("gstat.getSuperstepCounter():\t" + gstat.getSuperstepCounter().getValue());		
		System.out.println("gstat.getVertices():\t\t" + gstat.getVertices().getValue());
		System.out.println("gstat.getEdges():\t\t" + gstat.getEdges().getValue());
		System.out.println("gstat.getSentMessages():\t" + gstat.getSentMessages().getValue());
		System.out.println("gstat.getSentMessageBytes():\t" + gstat.getSentMessageBytes().getValue());
		System.out.println("gstat.getAggregateSentMessages():\t" + gstat.getAggregateSentMessages().getValue());
		System.out.println("gstat.getAggregateSentMessageBytes():\t" + gstat.getAggregateSentMessageBytes().getValue());
		System.out.println("getComputation():\t\t" + getComputation());
		System.out.println("workerAggregator:\t\t" + workerAgg);
		System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++");		
		System.out.println("getSuperstep():\t" + getSuperstep());		
		System.out.println("getTotalNumVertices():\t" + getTotalNumVertices());
		System.out.println("getTotalNumEdges():\t" + getTotalNumEdges());
		
//		System.out.println("GiraphStats.AGGREGATE_SENT_MESSAGES_NAME : " + GiraphStats.AGGREGATE_SENT_MESSAGES_NAME);
//		System.out.println(GiraphStats.AGGREGATE_SENT_MESSAGES_NAME);
		
		///prevent isolated loops that cause to repeat list ranking
		///isolated loops cause the equal message counts in some supersteps until the Rank overflows the integer type
		if ((getComputation() == DBGProcessComputation_ListRankingByPPA.class) || (getComputation() == DBGProcessComputation_ListRankingByPPA_2.class)) 			
			if (getSuperstep() > 10) 
				if (this.msgcnt != gstat.getSentMessages().getValue()) {
					this.msgcnt = gstat.getSentMessages().getValue();
					this.msgCntFlag = false;
				}
				else
					if (this.msgCntFlag == false)
						this.msgCntFlag = true;
					else /// halt computation if in 3 supersteps number of sent messages are equal
						if (getSuperstep() % 2 == 0) { /// merge computation should start in even supersteps
							System.out.println("!!! Loop Found !!!");
							setComputation(DBGProcessComputation_Merge.class);
							if (! this.mergeFlag)
								this.mergeFlag = true;
							else
								this.merge2Flag = true;
						} 

		// iterating on tip removal until no tip for remove exists
		int tipCount = 0;
		if (getComputation() == DBGProcessComputation_Tip_noKarect.class) {
			tipCount = ((IntWritable) getAggregatedValue(tipCountAgg)).get();
			if (getSuperstep() % 2 == 1)
				System.out.println("Removed Tip Count: " + tipCount);
			else
				System.out.println("Candid Tip Count: " + tipCount);
			if (tipCount > 0) {
				setAggregatedValue(tipCountAgg, new IntWritable(0));
//				setComputation(DBGProcessComputation_Tip_noKarect.class);
				this.tipFlag = false;
			}
		}
		
		/*
		 * 
		 */
		int bubleRemovedCount = 0;
		if (getComputation() == DBGProcessComputation_Bubble.class) {
			bubleRemovedCount = ((IntWritable) getAggregatedValue(tipCountAgg)).get();
			System.out.println("Removed Bubble: " + bubleRemovedCount);
		}
		
		/*
		 * 
		 */
		int branchCount = 0;
		if (getComputation() == DBGProcessComputation_SolveBranches.class) {
			branchCount = ((IntWritable) getAggregatedValue(tipCountAgg)).get();
			System.out.println("Detected Branches: " + branchCount);
		}

		
		
		if (getSuperstep() == 0)
			setComputation(DBGProcessComputation_ListRankingByPPA.class);		
				
		else if ((getSuperstep() > 4) && (workerAgg == true) && (this.mergeFlag == false)) {
			System.out.println("workerAggregator in start of merge:\t\t" + workerAgg);
//			haltComputation();
			setComputation(DBGProcessComputation_Merge.class);
//			broadcast("brdVar_kmerSize", new LongWritable(kmerSize));
			this.mergeFlag = true;
		}		
		
		else if ((getSuperstep() > 6) && (workerAgg == true) && (this.tipFlag == false)) {
//			superStep = (int) getSuperstep();
//			haltComputation();
			setComputation(DBGProcessComputation_Tip_noKarect.class);
			this.tipFlag = true;
		}
		
//		else if ((superStep > 0) && (getSuperstep() > superStep + 1) && (this.bubFlag == false)) {
		else if ((this.tipFlag) && (tipCount == 0) && (this.bubFlag == false)) {
//			haltComputation();
			superStep = (int) getSuperstep();
			setComputation(DBGProcessComputation_Bubble.class);
			this.bubFlag = true;
			
		}
		
		else if ((superStep > 0) && (getSuperstep() > superStep + 2) && (this.rank2Flag == false)) {
//			haltComputation();
			superStep = (int) getSuperstep();
//			broadcast("brdVar_rank2", new LongWritable(getSuperstep()));
			setComputation(DBGProcessComputation_ListRankingByPPA_2.class);
			this.rank2Flag = true;
		}	
		
		else if ((superStep > 0) && (getSuperstep() > superStep + 2) && (workerAgg == true) && (this.merge2Flag == false)) {
//		else if ( (getSuperstep() > 1) && (workerAgg == true) && (this.merge2Flag == false)) {
//			haltComputation();
			// merge computation must start in even superstep
			if (getSuperstep() % 2 == 0) {						
				setComputation(DBGProcessComputation_Merge.class);
//				broadcast("brdVar_kmerSize", new LongWritable(kmerSize));
				this.merge2Flag = true;
				
			}
		}
		
		else if ((workerAgg == true) && (this.merge2Flag == true) && (this.branchFlag == false)) {
			superStep = (int) getSuperstep();
			setComputation(DBGProcessComputation_SolveBranches.class);
			this.branchFlag = true;
		}
		
		else if ((this.branchFlag == true) && (getSuperstep() > superStep + 3)) {
			haltComputation();
		}
		
				
		// in the rank2 step, number of first superstep must be broadcasted  
//		if (this.rank2Flag == true && this.merge2Flag == false) {
		if (getComputation() == DBGProcessComputation_ListRankingByPPA_2.class) {
			broadcast("brdVar_rank2", new LongWritable(superStep));
		}
		
		// in merge1, tip and merge2 computations, kmersize must be broadcasted		
		if ((this.mergeFlag && !this.bubFlag) || (this.merge2Flag)) {
			broadcast("brdVar_kmerSize", new LongWritable(kmerSize));
//			System.out.println("broadcast kmerSize: " + kmerSize);
		}
		
		System.out.println("--------------------------------------------");
		System.out.println("isHalted: " + isHalted());
		System.out.println("Current Superstep getSuperstep():\t" + getSuperstep());		
		System.out.println("Current Computation, getComputation(): " + getComputation());
		
		System.gc();
		
	}

	public void readFields(DataInput dataInput) throws IOException {
		// To deserialize this class fields (global variables) if any		
		
//		Text tmpTxt = new Text();
//		tmpTxt.readFields(dataInput);
//		this.mergeAgg = tmpTxt.toString();
		
	}

	public void write(DataOutput dataOutput) throws IOException {
		// To serialize this class fields (global variables) if any
		
//		Text tmpTxt = new Text();
//	    tmpTxt.set(this.mergeAgg);
//	    tmpTxt.write(dataOutput);
	}	
	
}
