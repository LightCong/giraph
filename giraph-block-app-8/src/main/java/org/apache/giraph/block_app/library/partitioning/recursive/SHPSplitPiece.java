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

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.global_comm.array.BroadcastArrayHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.array.ReducerArrayHandle;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.library.partitioning.SHPExecutionStage;
import org.apache.giraph.block_app.library.partitioning.assignment.BucketAssigner;
import org.apache.giraph.block_app.library.partitioning.vertex.SocialHashPartitionerVertexValue;
import org.apache.giraph.block_app.reducers.array.BasicArrayReduce;
import org.apache.giraph.function.primitive.func.Int2IntFunction;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.giraph.types.NoMessage;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;


/**
 * Piece which splits each bucket into set of sub-buckets.
 *
 * Does so by updating candidate and current bucket appropriately.
 *
 * @param <V> vertex value
 */
public class SHPSplitPiece<V extends SocialHashPartitionerVertexValue> extends
    Piece<LongWritable, V, Writable, NoMessage, SHPExecutionStage> {

  private final RecursiveSettings recursiveSettings;
  private final BucketAssigner<V> bucketAssigner;

  private ReducerArrayHandle<LongWritable, LongWritable> bucketSizeReducer;
  private BroadcastArrayHandle<LongWritable> bucketSizeBroadcast;

  public SHPSplitPiece(
      RecursiveSettings recursiveSettings, BucketAssigner<V> bucketAssigner) {
    this.recursiveSettings = recursiveSettings.createCopy();
    this.bucketAssigner = bucketAssigner;
  }

  @Override
  public void registerReducers(
      CreateReducersApi reduceApi, SHPExecutionStage executionStage) {
    bucketSizeReducer = BasicArrayReduce.createArrayHandles(
      executionStage.getNumBuckets(),
      LongTypeOps.INSTANCE,
      SumReduce.LONG,
      reduceApi::createLocalReducer
    );
  }

  @Override
  public VertexSender<LongWritable, V, Writable> getVertexSender(
    BlockWorkerSendApi<LongWritable, V, Writable, NoMessage> workerApi,
    SHPExecutionStage executionStage
  ) {
    return (vertex) -> {
      V value = vertex.getValue();
      if (!value.getShouldVertexBeAssignedBucket()) {
        return;
      }

      int candidateBucket = value.getCandidateBucket();
      if (candidateBucket != SocialHashPartitionerVertexValue.BUCKET_NOT_SET) {
        reduceLong(bucketSizeReducer.get(candidateBucket), 1);
      }
    };
  }

  @Override
  public void masterCompute(
      BlockMasterApi masterApi, SHPExecutionStage executionStage) {
    bucketSizeBroadcast = bucketSizeReducer.broadcastValue(masterApi);
  }

  @Override
  public
  VertexReceiver<LongWritable, V, Writable, NoMessage> getVertexReceiver(
    BlockWorkerReceiveApi<LongWritable> workerApi,
    SHPExecutionStage executionStage
  ) {
    int splitEachIntoNumBuckets =
        RecursiveSettings.SPLIT_EACH_INTO_NUM_BUCKETS;

    Int2IntFunction toBucketAtCurrentLevel =
        recursiveSettings.getPreviousBucketToBucketAtLevel(
            executionStage.getSplits());
    Int2IntFunction toBucketAtNextLevel =
        recursiveSettings.getPreviousBucketToBucketAtLevel(
            executionStage.getSplits() + 1);

    return (vertex, messages) -> {
      V vertexValue = vertex.getValue();
      vertexValue.reset();

      if (!vertexValue.getShouldVertexBeAssignedBucket()) {
        return;
      }

      if (toBucketAtNextLevel != null) {
        int previousBucket = vertexValue.getPreviousLastLevelBucket();
        if (previousBucket !=
            SocialHashPartitionerVertexValue.BUCKET_NOT_SET) {
          vertexValue.setInitialBucket(
              toBucketAtNextLevel.apply(previousBucket), true);
        }
      }

      int candidateBucket = vertexValue.getCandidateBucket();
      if (candidateBucket != SocialHashPartitionerVertexValue.BUCKET_NOT_SET) {
        int beginBucket = candidateBucket * splitEachIntoNumBuckets;
        int endBucket =
            candidateBucket * splitEachIntoNumBuckets +
            splitEachIntoNumBuckets;
        long numVertices = bucketSizeBroadcast.get(candidateBucket)
            .getBroadcast(workerApi).get();
        int newCandidate = bucketAssigner.getAssignedBucket(
          vertexValue,
          beginBucket,
          endBucket,
          numVertices
        );
        if (toBucketAtCurrentLevel != null) {
          int previousBucket = vertexValue.getPreviousLastLevelBucket();
          if (previousBucket !=
              SocialHashPartitionerVertexValue.BUCKET_NOT_SET) {
            int predictedBucket = toBucketAtCurrentLevel.apply(previousBucket);
            int nextBucket = toBucketAtNextLevel.apply(previousBucket);
            if (candidateBucket == predictedBucket) {
              newCandidate = nextBucket;
            }
          }
        }
        Preconditions.checkState(
            candidateBucket * splitEachIntoNumBuckets <= newCandidate &&
            newCandidate < (candidateBucket + 1) * splitEachIntoNumBuckets);
        vertexValue.setCandidateBucket(newCandidate);
      }
    };
  }

  @Override
  public SHPExecutionStage nextExecutionStage(
      SHPExecutionStage executionStage) {
    return executionStage.changedSplits(executionStage.getSplits() + 1).
        changedNumBuckets(
            executionStage.getNumBuckets() *
            RecursiveSettings.SPLIT_EACH_INTO_NUM_BUCKETS).
        changedLastSplitIteration(executionStage.getIteration());
  }
}
