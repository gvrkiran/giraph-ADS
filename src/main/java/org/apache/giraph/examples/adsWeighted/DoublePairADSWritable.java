package org.apache.giraph.examples.adsWeighted;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

// public class DoublePairADSWritable extends DoublePairADS implements Writable, Configurable {
public class DoublePairADSWritable implements Writable, Configurable {

    private Configuration conf;
    double hash;
    double distance;
    long id;
    double mesgFlag;
    private Map<Double, Double> vertexADS = new HashMap<Double, Double>();

    public DoublePairADSWritable() {
        this.hash = 0;
        this.distance = 0;
        this.id = 0;
        this.mesgFlag = 0;
        vertexADS = new HashMap<Double, Double>();
    }

    public DoublePairADSWritable(double hash, double distance, long id) {
    	this.hash = hash;
    	this.distance = distance;
    	this.id = id;
    	this.mesgFlag = 0;
    	this.vertexADS.put(hash, 0d);
    }
    
    public DoublePairADSWritable(double hash, double distance, long id, double mesgFlag, Map<Double, Double> vertexADS) {
    	this.hash = hash;
    	this.distance = distance;
    	this.id = id;
    	this.mesgFlag = mesgFlag;
    	this.vertexADS = vertexADS;
    }
    
    public void setHash(double hash) {
        this.hash = hash;
    }

    public double getHash() {
        return hash;
    }
    
    public void setDistance(double distance) {
        this.distance = distance;
    }
    
    public double getDistance() {
        return distance;
    }
    
    public void setId(long id) {
    	this.id = id;
    }
    
    public long getId() {
    	return id;
    }
    
    public void setMesgFlag(double flag) {
    	this.mesgFlag = flag;
    }
    
    public double getMesgFlag() {
    	return mesgFlag;
    }
    
    public void setADS(double id) {
		vertexADS.put(id,0.0);
	}
	
	public Map<Double, Double> getADS() {
		return vertexADS;
	}
    
    @Override
    public void readFields(DataInput input) throws IOException {
        setHash(input.readDouble());
        setDistance(input.readDouble());
        setId(input.readLong());
        setMesgFlag(input.readDouble());
        
        int size = input.readInt();
		
		for (int i = 0; i < size; i++) {
		      this.vertexADS.put(
		    		  input.readDouble(), input.readDouble());
		}
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeDouble(getHash());
        output.writeDouble(getDistance());
        output.writeLong(getId());
        output.writeDouble(getMesgFlag());
        
        output.writeInt(this.vertexADS.size());

		for (Entry<Double, Double> entry : this.vertexADS.entrySet()) {
		      output.writeDouble(entry.getKey());
		      output.writeDouble(entry.getValue());
		}
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public String toString() {
        return getHash() + "," + getDistance();
    }
}