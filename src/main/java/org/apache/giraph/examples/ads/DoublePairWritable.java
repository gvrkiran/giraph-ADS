package org.apache.giraph.examples.ads;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
// import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

public class DoublePairWritable extends DoublePair implements Writable, Configurable {

    private Configuration conf;
    double first;
    double second;

    public DoublePairWritable() {
        super(0, 0);
    }

    public DoublePairWritable(double fst, double snd) {
    	// this.first = fst;
    	// this.second = snd;
        super(fst, snd);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.setFirst(input.readDouble());
        super.setSecond(input.readDouble());
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeDouble(super.getFirst());
        output.writeDouble(super.getSecond());
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
        return super.getFirst() + "," + super.getSecond();
    }
}

