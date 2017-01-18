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
package org.apache.giraph.block_app.library.partitioning;

/**
 * Execution stage for SHP calculation
 */
public class SHPExecutionStageImpl implements SHPExecutionStage {
  private final int iteration;
  private final int splits;
  private final int numBuckets;
  private final int lastSplitIteration;

  public SHPExecutionStageImpl() {
    this.iteration = 0;
    this.splits = 0;
    this.numBuckets = 0;
    this.lastSplitIteration = 0;
  }

  public SHPExecutionStageImpl(
      int iteration, int splits, int numBuckets, int lastSplitIteration) {
    this.iteration = iteration;
    this.splits = splits;
    this.numBuckets = numBuckets;
    this.lastSplitIteration = lastSplitIteration;
  }

  @Override
  public int getIteration() {
    return iteration;
  }

  @Override
  public int getSplits() {
    return splits;
  }

  @Override
  public int getNumBuckets() {
    return numBuckets;
  }

  @Override
  public SHPExecutionStageImpl changedIteration(int iteration) {
    return new SHPExecutionStageImpl(
        iteration, this.splits, this.numBuckets, this.lastSplitIteration);
  }

  @Override
  public SHPExecutionStageImpl changedSplits(int splits) {
    return new SHPExecutionStageImpl(
        this.iteration, splits, this.numBuckets, this.lastSplitIteration);
  }

  @Override
  public SHPExecutionStageImpl changedNumBuckets(int numBuckets) {
    return new SHPExecutionStageImpl(
        this.iteration, this.splits, numBuckets, this.lastSplitIteration);
  }

  @Override
  public int getLastSplitIteration() {
    return lastSplitIteration;
  }

  @Override
  public SHPExecutionStage changedLastSplitIteration(int lastSplitIteration) {
    return new SHPExecutionStageImpl(
        this.iteration, this.splits, this.numBuckets, lastSplitIteration);
  }

  @Override
  public int getIterationsFromSplit() {
    return this.iteration - this.lastSplitIteration;
  }

  @Override
  public String toString() {
    return "BPExecutionStage [iteration=" + iteration +
        ", iterationFromSplit=" + getIterationsFromSplit() + ", splits=" +
        splits + ", numPartitions=" + numBuckets + "]";
  }
}
