package example;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.hadoop.io.BooleanWritable;

public class MergeAggregator implements Aggregator<BooleanWritable>{

	BooleanWritable myValue;

	public void aggregate(BooleanWritable value) {
		myValue.set(myValue.get() && value.get());
	}

	public BooleanWritable createInitialValue() {
		return new BooleanWritable(false);
	}

	public BooleanWritable getAggregatedValue() {
		// TODO Auto-generated method stub
		return myValue;
	}

	public void setAggregatedValue(BooleanWritable value) {
		myValue.set(value.get());
	}

	public void reset() {
//		if (myValue.get() == false) {
//			myValue = new BooleanWritable(true);
//		} else {
//			myValue.set(true);
//		}
	}


}
