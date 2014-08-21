package org.apache.giraph.examples.facilityAlgorithm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;


// aggregator hashmap to contain nodes that were already frozen
public class FacilityLocationGiraphFreezeAggregator extends BasicAggregator<MapWritable>{
	
	@Override
	public void aggregate(MapWritable map) {
		Set<Double> map1 = getAggregatedValue().getFrozenNodes();
		Set<Double> map2 = map.getFrozenNodes();
		Set<Double> map3 = new HashSet<Double>();
		
		map3.addAll(map1);
		map3.addAll(map2);
		
		getAggregatedValue().setFrozenNodes(map3);
	}

	@Override
	public MapWritable createInitialValue() {
		// Map<Double, Double> frozenNodes = new HashMap<Double, Double>();
		return new MapWritable();
	}

}
