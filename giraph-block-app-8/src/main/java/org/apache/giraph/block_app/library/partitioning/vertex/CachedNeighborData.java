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
package org.apache.giraph.block_app.library.partitioning.vertex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.block_app.library.partitioning.goal.VertexBucketMessage;
import org.apache.giraph.function.primitive.comparators.IntComparatorFunction;
import org.apache.giraph.types.ops.collections.array.WIntArrayList;
import org.apache.giraph.types.ops.collections.array.WLongArrayList;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

/** Vertex state responsible for tracking data about neighbors */
public class CachedNeighborData implements Writable {
  private WLongArrayList ids = null;
  private WIntArrayList buckets = null;

  public CachedNeighborData() {
  }

  public CachedNeighborData(Iterator<VertexBucketMessage> updates) {
    ids = new WLongArrayList();
    buckets = new WIntArrayList();

    while (updates.hasNext()) {
      VertexBucketMessage next = updates.next();
      ids.add(next.getId());
      buckets.add(next.getBucket());
    }

    ids.trim();
    buckets.trim();
  }

  public void update(Iterator<VertexBucketMessage> updates) {
    Long2IntOpenHashMap updateMap = new Long2IntOpenHashMap();
    updateMap.defaultReturnValue(-2);
    while (updates.hasNext()) {
      VertexBucketMessage next = updates.next();
      updateMap.put(next.getId(), next.getBucket());
    }

    for (int i = 0; i < ids.size(); i++) {
      int bucket = updateMap.remove(ids.getLong(i));
      if (bucket != -2) {
        buckets.set(i, bucket);
      }
    }

    Preconditions.checkState(updateMap.isEmpty());
  }

  public void sortByBucket() {
    Arrays.quickSort(
        0,
        buckets.size(),
        (IntComparatorFunction) (i, j) ->
          Integer.compare(buckets.getInt(i), buckets.getInt(j)),
        (i, j) -> {
          ids.swap(i, j);
          buckets.swap(i, j);
        });
  }

  public int size() {
    return buckets.size();
  }

  public int getBucket(int index) {
    return buckets.getInt(index);
  }

  public long getId(int index) {
    return ids.getLong(index);
  }

  public void fillIdSubset(WLongArrayList targets, int start, int end) {
    targets.clear();
    targets.addElements(0, ids.elements(), start, end - start);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    ids = WLongArrayList.readNew(in);
    buckets = WIntArrayList.readNew(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    ids.write(out);
    buckets.write(out);
  }

}
