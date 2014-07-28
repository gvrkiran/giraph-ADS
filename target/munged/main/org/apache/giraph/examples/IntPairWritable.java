package org.apache.giraph.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

public class IntPairWritable extends IntPair implements Writable, Configurable {

    private Configuration conf;

    public IntPairWritable() {
        super(0, 0);
    }

    public IntPairWritable(int fst, int snd) {
        super(fst, snd);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.setFirst(input.readInt());
        super.setSecond(input.readInt());
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(super.getFirst());
        output.writeInt(super.getSecond());
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
