package org.apache.giraph.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

public class FacilityLocationTestMessage extends FacilityLocationTestMessageType implements Writable, Configurable {

    private Configuration conf;
    private HyperLogLog counter = null;

    public FacilityLocationTestMessage() {
        super(0l,0d,null);
    }

    public FacilityLocationTestMessage(long id, double distance, HyperLogLog counter) {
        super(id,distance,counter);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.setId(input.readLong());
        super.setDistance(input.readDouble());
        super.setCounter(input.readLong());
    }

    @Override
    public void write(DataOutput output) throws IOException {
    	// output.writeBytes(s);
        // output.writeInt(super.getFirst());
        // output.writeInt(super.getSecond());
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

}
