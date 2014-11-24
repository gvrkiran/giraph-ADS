package org.apache.giraph.examples.luby;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.Text;

/**
 * Aggregator that stores a value that is overwritten once another value is
 * aggregated. This aggregator is useful for one-to-many communication from
 * master.compute() or from a special vertex. In case multiple vertices write
 * to this aggregator, its behavior is non-deterministic.
 */
public class TextOverwriteAggregator extends BasicAggregator<Text> {

	private Text value;

	public TextOverwriteAggregator() {
		value = createInitialValue();
	}

	public TextOverwriteAggregator(Text initialValue) {
		value = initialValue;
	}

	@Override
	public void aggregate(Text value) {
		getAggregatedValue().set(value);
	}

	@Override
	public Text createInitialValue() {
		return new Text("");
	}

  	@Override
  	public Text getAggregatedValue() {
	  	return value;
  	}

  	@Override
  	public void setAggregatedValue(Text value) {
	  	this.value = value;
  	}
}