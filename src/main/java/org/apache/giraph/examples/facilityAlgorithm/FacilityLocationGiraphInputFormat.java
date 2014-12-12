package org.apache.giraph.examples.facilityAlgorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FacilityLocationGiraphInputFormat extends TextVertexInputFormat<LongWritable, FacilityLocationGiraphVertexValue, FloatWritable> {
	
	public Set<Long> verticesOnThisMachine = new HashSet<Long> ();
	
	@Override
	public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new FacilityLocationGiraphVertexReaderFromEachLine();
	}

	/**
	* Reads the line and parses them by the following schema:
	* vertexID \t <code> double vertex hash value </code> \t
	* <code>; delimited id's of neighbors:edge weights</code>
	*/
	
	private class FacilityLocationGiraphVertexReaderFromEachLine extends TextVertexReaderFromEachLine {

		@Override
		protected Iterable<Edge<LongWritable, FloatWritable>> getEdges(Text line) throws IOException {
				String[] splitLine = line.toString().split("\t");
				String[] connectedVertexIds = splitLine[2].split(";");
				
				List<Edge<LongWritable, FloatWritable>> edges = new
			              ArrayList<Edge<LongWritable, FloatWritable>>();
				
				for (int i = 0; i < connectedVertexIds.length; i++) {
					String[] temp1 = connectedVertexIds[i].split(":");
			        long targetId = Long.parseLong(temp1[0]);
			        float edgeWeight = Float.parseFloat(temp1[1]);
			        edges.add(EdgeFactory.create(new LongWritable(targetId),new FloatWritable(edgeWeight)));
			      }

			      return edges;
		}

		@Override
		protected LongWritable getId(Text line) throws IOException {
			String[] splitLine = line.toString().split("\t");
			long id = Long.parseLong(splitLine[0]);
			verticesOnThisMachine.add(id);
			return new LongWritable(id);
		}

		@Override
		protected FacilityLocationGiraphVertexValue getValue(Text line) throws IOException {
				FacilityLocationGiraphVertexValue value = new FacilityLocationGiraphVertexValue();
				String[] splitLine = line.toString().split("\t");
				String facilityCostStr = splitLine[1];
				double facilityCost = Double.parseDouble(facilityCostStr);
				value.setFacilityCost(facilityCost);
				
				String vertexADS = splitLine[3];
				
				/*
				Map<Double, Double> ads = new HashMap<Double, Double> ();
				
				for (int i = 0; i < vertexADS.length; i++) {
					String[] temp1 = vertexADS[i].split(":");
			        double hash = Double.parseDouble(temp1[0]);
			        double distance = Double.parseDouble(temp1[1]);
			        ads.put(hash,distance);
			      }
				*/
				
				value.setADS(vertexADS);
				
				return value;
		}
		
	}
}
