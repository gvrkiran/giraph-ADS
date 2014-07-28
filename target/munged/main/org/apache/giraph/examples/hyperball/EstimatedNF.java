/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.examples.hyperball;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EstimatedNF implements Writable {

  private HyperLogLog counter;

  private ByteArrayList hops = new ByteArrayList();
  private LongArrayList nf = new LongArrayList();
  private double rsd = 0.01;
  
  public EstimatedNF() {
    counter = new HyperLogLog(rsd);
  }

  public HyperLogLog counter() {
    return counter;
  }

  public void registerEstimate(byte hop, long numReachableVertices) {
    hops.add(hop);
    nf.add(numReachableVertices);
  }

  @Override
  public void write(DataOutput out) throws IOException {

    // counter.write(out);

    int size = nf.size();
    out.writeInt(size);
    for (int n = 0; n < size; n++) {
      out.writeByte(hops.getByte(n));
      out.writeLong(nf.getLong(n));
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // counter.readFields(in);
    int size = in.readInt();
    byte[] hopElems = new byte[size];
    long[] nfElems = new long[size];
    for (int n = 0; n < size; n++) {
      hopElems[n] = in.readByte();
      nfElems[n] = in.readLong();
    }
    hops = new ByteArrayList(hopElems);
    nf = new LongArrayList(nfElems);
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    int size = nf.size();
    for (int n = 0; n < size; n++) {
      byte hop = hops.getByte(n);
      long numReachableVertices = nf.getLong(n);
      buffer.append(hop).append(":")
            .append(numReachableVertices).append(";");
      n++;
    }
    return buffer.toString();
  }
}
