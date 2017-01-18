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
package org.apache.giraph.block_app.library.partitioning.recursive;

import org.apache.giraph.block_app.library.partitioning.vertex.SocialHashPartitionerVertexValue;
import org.apache.giraph.function.primitive.func.Int2IntFunction;

import com.google.common.base.Preconditions;

/**
 * Recursive settings
 */
public class RecursiveSettings {
  public static final int SPLIT_EACH_INTO_NUM_BUCKETS = 2;

  protected int initialNumBuckets = 1;
  /** The number of buckets to divide each existing bucket into. */
  protected int numSplits = 0;
  /** The number of splits that have already occurred in the initial bucket. */
  protected int priorNumSplits = 0;

  /** Whether to use initialization from a previous partitioning job */
  protected boolean usePreviousPartitioning = false;

  /** Number of recursive levels used in the previous partitioning */
  protected int previousPartitioningNumLevels = 0;

  /** When using recursive splitting, should we split initially */
  protected boolean runIterationsBeforeFirstSplit = false;

  public boolean isRecursiveSplitting() {
    return getNumSplits() > 0 || getPriorNumSplits() > 0;
  }

  public int getInitialNumBuckets() {
    return initialNumBuckets;
  }

  public int getFinalNumBuckets() {
    return (int) (initialNumBuckets * Math.pow(2, numSplits));
  }

  public int getNumSplits() {
    Preconditions.checkState(numSplits >= 0);
    return numSplits;
  }

  public int getPriorNumSplits() {
    Preconditions.checkState(priorNumSplits >= 0);
    return priorNumSplits;
  }

  public boolean runIterationsBeforeFirstSplit() {
    Preconditions.checkState(runIterationsBeforeFirstSplit || numSplits > 0);
    return runIterationsBeforeFirstSplit;
  }

  public Int2IntFunction getPreviousBucketToBucketAtLevel(int level) {
    if (usePreviousPartitioning) {
      Preconditions.checkState(isRecursiveSplitting());
      Preconditions.checkState(previousPartitioningNumLevels > 0);
      int divisor = (int) Math.pow(SPLIT_EACH_INTO_NUM_BUCKETS,
          previousPartitioningNumLevels - level - getPriorNumSplits());
      return (previousBucket) -> {
        Preconditions.checkState(
            previousBucket != SocialHashPartitionerVertexValue.BUCKET_NOT_SET);
        return previousBucket / divisor;
      };
    } else {
      return null;
    }
  }

  public static String getCodeSnippet(
    int numSplits,
    int splitEachIntoNumBuckets,
    int priorNumSplits,
    int splitMovementDepth,
    boolean runIterationsBeforeFirstSplit
  ) {
    return "numSplits = " + numSplits + ";" +
      "splitEachIntoNumBuckets = " + splitEachIntoNumBuckets + ";" +
      "priorNumSplits = " + priorNumSplits + ";" +
      "splitMovementDepth = " + splitMovementDepth + ";" +
      "runIterationsBeforeFirstSplit = " + runIterationsBeforeFirstSplit + ";";
  }

  public RecursiveSettings createCopy() {
    RecursiveSettings copy = new RecursiveSettings();
    copy.numSplits = numSplits;
    copy.priorNumSplits = priorNumSplits;
    copy.usePreviousPartitioning = usePreviousPartitioning;
    copy.previousPartitioningNumLevels = previousPartitioningNumLevels;
    copy.runIterationsBeforeFirstSplit = runIterationsBeforeFirstSplit;
    return copy;
  }
}
