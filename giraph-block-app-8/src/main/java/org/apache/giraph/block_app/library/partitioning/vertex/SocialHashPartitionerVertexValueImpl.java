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

import java.util.BitSet;
import java.util.Iterator;

import org.apache.giraph.block_app.library.partitioning.goal.VertexBucketMessage;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.writable.kryo.KryoWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

/** Social Hash Partitioner Vertex Value Implementation */
public class SocialHashPartitionerVertexValueImpl extends KryoWritable
    implements SocialHashPartitionerVertexValue {
  private boolean hasInitialAssignment;
  private int initialBucket;

  private int previousLastLevelBucket;

  private int candidateBucket;
  private int currentBucket;

  private float bisectionMoveGain;

  private boolean shouldVertexBeAssignedBucket;

  private CachedNeighborData neighborData;
  private int removedIsolatedNeighbors;

  private BitSet uselessEdges;

  public SocialHashPartitionerVertexValueImpl() {
    initialize();
  }

  public void initialize() {
    this.currentBucket = BUCKET_NOT_SET;
    this.candidateBucket = BUCKET_NOT_SET;
    this.initialBucket = BUCKET_NOT_SET;
    this.hasInitialAssignment = false;
    this.shouldVertexBeAssignedBucket = false;
    this.neighborData = null;
    this.uselessEdges = null;
  }

  @Override
  public void reset() {
    currentBucket = BUCKET_NOT_SET;
    bisectionMoveGain = 0;
    neighborData = null;
  }

  @Override
  public int getCandidateBucket() {
    return candidateBucket;
  }

  @Override
  public void setCandidateBucket(int candidateBucket) {
    if (candidateBucket < 0 && candidateBucket != BUCKET_NOT_SET) {
      throw new IllegalArgumentException(
          "Candidate bucket cannot be " + candidateBucket);
    }
    this.candidateBucket = candidateBucket;
  }

  @Override
  public int getCurrentBucket() {
    return currentBucket;
  }

  @Override
  public void setCurrentBucket(int currentBucket) {
    if (currentBucket < 0 && currentBucket != BUCKET_NOT_SET) {
      throw new IllegalArgumentException(
          "Current bucket cannot be " + currentBucket);
    }
    this.currentBucket = currentBucket;
  }

  @Override
  public int getInitialBucket() {
    return initialBucket;
  }

  @Override
  public void setInitialBucket(
      int initialBucket, boolean hadAssignment) {
    Preconditions.checkState(initialBucket != BUCKET_NOT_SET);
    this.initialBucket = initialBucket;
    this.hasInitialAssignment = hadAssignment;
  }

  @Override
  public int getPreviousLastLevelBucket() {
    return previousLastLevelBucket;
  }

  @Override
  public void setBisectionMoveGain(float bisectionMoveGain) {
    this.bisectionMoveGain = bisectionMoveGain;
  }

  @Override
  public float getBisectionMoveGain() {
    return bisectionMoveGain;
  }

  @Override
  public void setShouldVertexBeAssignedBucket(
      boolean shouldVertexBeAssignedBucket) {
    this.shouldVertexBeAssignedBucket = shouldVertexBeAssignedBucket;
  }

  @Override
  public boolean getShouldVertexBeAssignedBucket() {
    return shouldVertexBeAssignedBucket;
  }

  @Override
  public CachedNeighborData updateNeighborData(
      Iterator<VertexBucketMessage> updates) {
    if (updates.hasNext()) {
      if (neighborData == null) {
        neighborData = new CachedNeighborData(updates);
      } else {
        neighborData.update(updates);
      }

      neighborData.sortByBucket();
    }
    return neighborData;
  }

  @Override
  public void initUselessEdgesSet(int numEdges) {
    uselessEdges = new BitSet(numEdges);
  }

  @Override
  public void markEdgeAsUseless(int index) {
    uselessEdges.set(index, true);
  }

  @Override
  public <I extends WritableComparable>
  Iterator<I> getUsefulEdgeIdIterator(
      Iterator<? extends Edge<I, ? extends Writable>> iterator) {
    return new AbstractIterator<I>() {
      private int index = 0;
      @Override
      protected I computeNext() {
        while (iterator.hasNext() && uselessEdges.get(index)) {
          index++;
          iterator.next();
        }
        if (iterator.hasNext()) {
          Preconditions.checkState(!uselessEdges.get(index));
          index++;
          return iterator.next().getTargetVertexId();
        } else {
          return endOfData();
        }
      }
    };
  }

  @Override
  public void incrementRemovedIsolatedNeighbors(int value) {
    removedIsolatedNeighbors += value;
  }

  @Override
  public int getRemovedIsolatedNeighbors() {
    return removedIsolatedNeighbors;
  }
}
