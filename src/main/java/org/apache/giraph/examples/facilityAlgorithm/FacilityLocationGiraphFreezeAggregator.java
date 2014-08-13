package org.apache.giraph.examples.facilityAlgorithm;

import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;


// aggregator to contain nodes that were already frozen
public class FacilityLocationGiraphFreezeAggregator extends BasicAggregator<MapWritable>{

	public Map<Double, Double> vertexADS = new HashMap<Double, Double>();
	
	@Override
	public void aggregate(MapWritable map) {
		Map<Double, Double> map1 = getAggregatedValue().getFrozenNodes();
		Map<Double, Double> map2 = map.getFrozenNodes();
		Map<Double, Double> map3 = new HashMap<Double, Double>();
		map3.putAll(map1);
		map3.putAll(map2);
		
		getAggregatedValue().setFrozenNodes(map3);
	}

	@Override
	public MapWritable createInitialValue() {
		// Map<Double, Double> frozenNodes = new HashMap<Double, Double>();
		return new MapWritable();
	}

}
