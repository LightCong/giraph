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

import java.util.Iterator;

import org.apache.giraph.block_app.library.partitioning.goal.VertexBucketMessage;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/** Social Hash Partitioner Vertex Value interface */
public interface SocialHashPartitionerVertexValue extends Writable {
  int BUCKET_NOT_SET = -1;

  int getCandidateBucket();
  void setCandidateBucket(int candidateBucket);

  int getCurrentBucket();
  void setCurrentBucket(int currentBucket);

  int getInitialBucket();
  void setInitialBucket(int initialBucket, boolean hadAssignment);

  int getPreviousLastLevelBucket();

  void setBisectionMoveGain(float gain);
  float getBisectionMoveGain();

  void reset();

  boolean getShouldVertexBeAssignedBucket();
  void setShouldVertexBeAssignedBucket(boolean shouldVertexBeAssignedBucket);

  CachedNeighborData updateNeighborData(Iterator<VertexBucketMessage> updates);

  void initUselessEdgesSet(int numEdges);
  void markEdgeAsUseless(int index);
  <I extends WritableComparable>
  Iterator<I> getUsefulEdgeIdIterator(
      Iterator<? extends Edge<I, ? extends Writable>> iterator);

  void incrementRemovedIsolatedNeighbors(int value);
  int getRemovedIsolatedNeighbors();
}
