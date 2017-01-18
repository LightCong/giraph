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
package org.apache.giraph.block_app.library.partitioning.assignment;

import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.block_app.library.partitioning.SHPExecutionStage;
import org.apache.giraph.block_app.library.partitioning.SocialHashPartitionerSettings;
import org.apache.giraph.block_app.library.partitioning.recursive.RecursiveSettings;
import org.apache.giraph.block_app.library.partitioning.vertex.SocialHashPartitionerVertexValue;
import org.apache.giraph.function.primitive.PrimitiveRefs;
import org.apache.giraph.function.primitive.func.Int2IntFunction;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.giraph.types.NoMessage;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

/**
 * Piece that initializes vertices, ignoring those that have initial bucket
 * in the input. Updates candidate and initial bucket on a vertex to
 * appropriate bucket.
 *
 * @param <V> Vertex value type
 */
public class SHPInitializePiece<V extends SocialHashPartitionerVertexValue>
    extends Piece<LongWritable, V, Writable, NoMessage, SHPExecutionStage> {

  private final int numInitialBuckets;
  private final BucketAssigner<V> bucketAssigner;
  private final PrimitiveRefs.LongRef numVerticesHolder;

  private SHPInitializePiece(
    int numInitialBuckets,
    BucketAssigner<V> bucketAssigner,
    PrimitiveRefs.LongRef numVerticesHolder
  ) {
    this.numInitialBuckets = numInitialBuckets;
    this.bucketAssigner = bucketAssigner;
    this.numVerticesHolder = numVerticesHolder;
  }

  public static <V extends SocialHashPartitionerVertexValue> Block create(
    int initialNumBuckets,
    BucketAssigner<V> bucketAssigner
  ) {
    LongWritable zero = new LongWritable(0);
    LongWritable one = new LongWritable(1);
    PrimitiveRefs.LongRef numVerticesHolder = new PrimitiveRefs.LongRef(0);
    Piece reduceNumVerticesPiece = Pieces.reduce(
      "ReduceNumVerticesPiece",
      SumReduce.LONG,
      (vertex) -> {
        return ((V) vertex.getValue()).getShouldVertexBeAssignedBucket() ?
          one : zero;
      },
      (result) -> {
        numVerticesHolder.value = result.get();
      }
    );

    return new SequenceBlock(
      reduceNumVerticesPiece,
      new SHPInitializePiece<>(
        initialNumBuckets,
        bucketAssigner,
        numVerticesHolder
      )
    );
  }

  @Override
  public VertexSender<LongWritable, V, Writable> getVertexSender(
    BlockWorkerSendApi<LongWritable, V, Writable, NoMessage> workerApi,
    SHPExecutionStage executionStage
  ) {
    RecursiveSettings recSettings =
        SocialHashPartitionerSettings.RECURSIVE.createObject(
            workerApi.getConf());
    Int2IntFunction toBucketAtLevel0 =
        recSettings.getPreviousBucketToBucketAtLevel(0);

    return (vertex) -> {
      V vertexValue = vertex.getValue();
      if (!vertexValue.getShouldVertexBeAssignedBucket()) {
        return;
      }
      final int notSet = SocialHashPartitionerVertexValue.BUCKET_NOT_SET;
      if (vertexValue.getCurrentBucket() != notSet ||
          vertexValue.getCandidateBucket() != notSet) {
        Preconditions.checkState(
            0 <= vertexValue.getCurrentBucket() &&
            vertexValue.getCurrentBucket() < numInitialBuckets);
        Preconditions.checkState(
            0 <= vertexValue.getCandidateBucket() &&
            vertexValue.getCandidateBucket() < numInitialBuckets);
        return;
      }

      if (toBucketAtLevel0 != null) {
        int previousBucket = vertexValue.getPreviousLastLevelBucket();
        if (previousBucket != notSet) {
          int initialBucket = toBucketAtLevel0.apply(previousBucket);
          Preconditions.checkState(
            0 <= initialBucket && initialBucket < numInitialBuckets,
            "Incorrect initialization of initialBucket - trying to assign " +
            "vertex: " + vertex.getId().get() + " a bucket of : " +
            initialBucket + " out of " + numInitialBuckets + " buckets"
          );
          vertexValue.setInitialBucket(initialBucket, true);
        }
      }

      // Assign initial partitions, if not specified in the input
      if (vertexValue.getInitialBucket() == notSet) {
        int assignedBucket = bucketAssigner.getAssignedBucket(
          vertexValue,
          0,
          numInitialBuckets,
          numVerticesHolder.value
        );
        Preconditions.checkState(
          assignedBucket != notSet,
          "Couldn't assign appropriate bucket to vertex " +
          vertex.getId().get()
        );
        vertexValue.setInitialBucket(assignedBucket, false);
      }

      vertexValue.setCandidateBucket(vertexValue.getInitialBucket());
    };
  }

  @Override
  public SHPExecutionStage nextExecutionStage(
      SHPExecutionStage executionStage) {
    return executionStage.changedIteration(0).changedSplits(0)
        .changedNumBuckets(numInitialBuckets);
  }
}
