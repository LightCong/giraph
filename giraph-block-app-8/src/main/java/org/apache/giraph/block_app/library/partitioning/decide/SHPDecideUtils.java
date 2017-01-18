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
package org.apache.giraph.block_app.library.partitioning.decide;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.global_comm.BroadcastHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.array.ReducerArrayHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.map.ReducerMapHandle;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.library.partitioning.SHPExecutionStage;
import org.apache.giraph.block_app.library.partitioning.SHPLoggingBuilder;
import org.apache.giraph.block_app.library.partitioning.SocialHashPartitionerSettings;
import org.apache.giraph.block_app.library.partitioning.recursive.MoveBucketLimiter;
import org.apache.giraph.block_app.library.partitioning.recursive.MoveBucketLimiterImpl;
import org.apache.giraph.block_app.library.partitioning.recursive.RecursiveSettings;
import org.apache.giraph.block_app.library.partitioning.vertex.SocialHashPartitionerVertexValue;
import org.apache.giraph.block_app.reducers.array.ArrayOfHandles;
import org.apache.giraph.block_app.reducers.array.BasicArrayReduce;
import org.apache.giraph.block_app.reducers.map.BasicMapReduce;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.function.primitive.func.Byte2LongFunction;
import org.apache.giraph.function.primitive.func.Int2FloatFunction;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.giraph.types.NoMessage;
import org.apache.giraph.types.ops.ByteTypeOps;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/** Utilities for deciding which vertices are going to move */
public class SHPDecideUtils {
  private static final Logger LOG = Logger.getLogger(SHPDecideUtils.class);

  private SHPDecideUtils() { }

  public static Block createDecideBlock(
      GiraphConfiguration conf, RecursiveSettings recursive) {
    HistogramDesc histogramDesc = HistogramDescImpl.create(
        SocialHashPartitionerSettings.GAIN_HISTOGRAM_NUM_QUARTER_BINS.get(conf),
        SocialHashPartitionerSettings.GAIN_HISTOGRAM_EXPONENT.get(conf));
    MoveBucketLimiter limiter = new MoveBucketLimiterImpl();

    float moveProbability =
        SocialHashPartitionerSettings.MOVE_PROBABILITY.get(conf);
    Preconditions.checkState(
        moveProbability <= 1 && moveProbability > 0);
    float maxMoveRatio =
        SocialHashPartitionerSettings.MAX_MOVE_RATIO.get(conf);
    Preconditions.checkState(maxMoveRatio > 0);
    float allowedImbalance =
        SocialHashPartitionerSettings.ALLOWED_IMBALANCE.get(conf);
    Preconditions.checkState(allowedImbalance >= 0);

    int numSplits = recursive.getNumSplits();
    Int2FloatFunction getAllowedImbalance;
    if (recursive.runIterationsBeforeFirstSplit()) {
      getAllowedImbalance =
        (split) -> allowedImbalance * (1 + split) / (1 + numSplits);
    } else {
      getAllowedImbalance = (split) -> allowedImbalance * split / numSplits;
    }

    return new DecideMovementPiece(
        histogramDesc, limiter, moveProbability, maxMoveRatio,
        getAllowedImbalance);
  }

  /** Piece to decide which vertices are going to move */
  public static final class DecideMovementPiece extends
      Piece<LongWritable, SocialHashPartitionerVertexValue, NullWritable,
        NoMessage, SHPExecutionStage> {
    private final HistogramDesc histogramDesc;
    private final MoveBucketLimiter limiter;
    private final float moveProbability;
    private final float maxMoveRatio;
    private final Int2FloatFunction getAllowedImbalance;

    private ReducerArrayHandle<LongWritable, LongWritable> bucketSizes;
    private
    ArrayOfHandles<ReducerMapHandle<ByteWritable, LongWritable, LongWritable>>
    gainHistograms;
    private BroadcastHandle<float[]> probsBroadcast;
    private BroadcastHandle<byte[]> binsBroadcast;

    public DecideMovementPiece(
        HistogramDesc histogramDesc, MoveBucketLimiter limiter,
        float moveProbability, float maxMoveRatio,
        Int2FloatFunction getAllowedImbalance) {
      this.histogramDesc = histogramDesc;
      this.limiter = limiter;
      this.moveProbability = moveProbability;
      this.maxMoveRatio = maxMoveRatio;
      this.getAllowedImbalance = getAllowedImbalance;
    }

    @Override
    public void registerReducers(
        CreateReducersApi reduceApi, SHPExecutionStage executionStage) {
      bucketSizes = BasicArrayReduce.createLocalArrayHandles(
          executionStage.getNumBuckets(),
          LongTypeOps.INSTANCE, SumReduce.LONG, reduceApi);

      gainHistograms = new ArrayOfHandles<>(
          executionStage.getNumBuckets(),
          () -> BasicMapReduce.createLocalMapHandles(
              ByteTypeOps.INSTANCE, LongTypeOps.INSTANCE, SumReduce.LONG,
              reduceApi));
    }

    @Override
    public
    VertexSender<LongWritable, SocialHashPartitionerVertexValue, NullWritable>
    getVertexSender(
        BlockWorkerSendApi<LongWritable, SocialHashPartitionerVertexValue,
          NullWritable, NoMessage> workerApi,
        SHPExecutionStage executionStage) {
      ByteWritable bin = new ByteWritable();
      return (vertex) -> {
        if (!vertex.getValue().getShouldVertexBeAssignedBucket()) {
          return;
        }
        Preconditions.checkState(
            vertex.getValue().getCandidateBucket() ==
            vertex.getValue().getCurrentBucket());
        int bucket = vertex.getValue().getCandidateBucket();
        float gain = vertex.getValue().getBisectionMoveGain();

        reduceLong(bucketSizes.get(bucket), 1);
        bin.set(histogramDesc.toBin(gain));

        if (SocialHashPartitionerSettings.IS_DEBUG) {
          LOG.info(String.format(
              "Vertex %d with gains %f assigned to bin %d, which is from %f",
              vertex.getId().get(), gain, bin.get(),
              histogramDesc.smallestValue(bin.get())));
        }
        reduceLong(
            gainHistograms.get(limiter.otherBucket(bucket)).get(bin), 1);
      };
    }

    @Override
    public void masterCompute(
        BlockMasterApi master, SHPExecutionStage executionStage) {
      long[] globalBucketSizes = new long[executionStage.getNumBuckets()];
      long totalSize = logImbalanceAndComputeTotalSize(
          master, executionStage, globalBucketSizes);

      final int left = 0;
      final int right = 1;

      float allowedSize =
          ((float) totalSize) / executionStage.getNumBuckets();
      float allowedImbalance =
          getAllowedImbalance.apply(executionStage.getSplits());
      if (allowedImbalance > 0) {
        allowedSize *= 1 + allowedImbalance;
      }

      int[] bucket = new int[2];
      long[] bucketSize = new long[2];

      float[] probs = new float[executionStage.getNumBuckets()];
      byte[] bins = new byte[executionStage.getNumBuckets()];

      ReducerMapHandle<ByteWritable, LongWritable, LongWritable>[] histogram =
          new ReducerMapHandle[2];

      Byte2LongFunction[] getMoveCandidates = new Byte2LongFunction[] {
        index -> getMappedByte(
            histogram[left], index).getReducedValue(master).get(),
        index -> getMappedByte(
            histogram[right], index).getReducedValue(master).get()
      };

      for (bucket[left] = 0; bucket[left] < executionStage.getNumBuckets();
          bucket[left] += 2) {
        Preconditions.checkState(
            bucket[left] == limiter.startBucket(bucket[left]));
        bucket[right] = limiter.endBucket(bucket[left]);

        boolean shouldLog =
            SocialHashPartitionerSettings.IS_DEBUG || bucket[right] < 4;

        for (int side = 0; side < 2; side++) {
          bucketSize[side] = globalBucketSizes[bucket[side]];

          histogram[side] = gainHistograms.get(bucket[side]);

          if (shouldLog) {
            StringBuilder sb = new StringBuilder(
                "Gains to bucket " + bucket[side] + ": ");
            for (byte bin = histogramDesc.smallestIndex();
                bin <= histogramDesc.largestIndex(); bin++) {
              sb.append(String.format(
                  "%4.3f+ %3d  ",
                  histogramDesc.smallestValue(bin),
                  getMoveCandidates[side].apply(bin)));
            }
            LOG.info(sb);
          }
        }

        histogramDesc.computeWhatToSwap(
            bucket, bucketSize, allowedSize, moveProbability, maxMoveRatio,
            getMoveCandidates, bins, probs);

        if (shouldLog) {
          for (int side = 0; side < 2; side++) {
            byte bin = bins[bucket[side]];
            LOG.info(String.format(
                "Decided to move to %d: from bin %d (>=%f) with prob %f",
                bucket[side], bin,
                bin <= histogramDesc.largestIndex() ?
                  histogramDesc.smallestValue(bin) :
                  Float.POSITIVE_INFINITY, probs[bucket[side]]));
          }
        }
      }
      probsBroadcast = master.broadcast(probs);
      binsBroadcast = master.broadcast(bins);
    }

    private long logImbalanceAndComputeTotalSize(
        BlockMasterApi master, SHPExecutionStage executionStage,
        long[] globalBucketSizes) {
      long totalSize = 0;

      long maxBucketSize = 0;
      long maxBucketSizeIndex = 0;
      long sumBucketSize = 0;

      for (int i = 0; i < executionStage.getNumBuckets(); i++) {
        globalBucketSizes[i] =
            bucketSizes.get(i).getReducedValue(master).get();
        totalSize += globalBucketSizes[i];

        if (maxBucketSize <  globalBucketSizes[i]) {
          maxBucketSize =  globalBucketSizes[i];
          maxBucketSizeIndex =  i;
        }
        maxBucketSize = Math.max(maxBucketSize,  globalBucketSizes[i]);
        sumBucketSize += globalBucketSizes[i];
      }

      float imbalance = maxBucketSize /
          (((float) sumBucketSize) / executionStage.getNumBuckets());
      SHPLoggingBuilder.setCounter(
          "imbalance", imbalance, master, executionStage);

      new SHPLoggingBuilder()
        .appendLine(
            "Current imbalance [" + maxBucketSizeIndex + "]: " + imbalance +
            " = " + maxBucketSize + " / " + sumBucketSize + " / " +
            executionStage.getNumBuckets())
        .logToCommandLine(master);
      return totalSize;
    }

    @Override
    public
    VertexReceiver<LongWritable, SocialHashPartitionerVertexValue,
    NullWritable, NoMessage>
    getVertexReceiver(BlockWorkerReceiveApi<LongWritable> workerApi,
        SHPExecutionStage executionStage) {
      return (vertex, messages) -> {
        if (!vertex.getValue().getShouldVertexBeAssignedBucket()) {
          return;
        }

        float gain = vertex.getValue().getBisectionMoveGain();
        byte bin = histogramDesc.toBin(gain);

        int targetBucket =
            limiter.otherBucket(vertex.getValue().getCandidateBucket());

        byte moveBin = binsBroadcast.getBroadcast(workerApi)[targetBucket];
        float prob;
        if (moveBin < bin) {
          prob = 1;
        } else if (moveBin == bin) {
          prob = probsBroadcast.getBroadcast(workerApi)[targetBucket];
        } else {
          prob = 0;
        }
        prob *= moveProbability;

        if (0 < prob &&
            (1 <= prob || ThreadLocalRandom.current().nextFloat() < prob)) {
          if (SocialHashPartitionerSettings.IS_DEBUG) {
            LOG.info(
                "Moving " + vertex.getId() + " from " +
                vertex.getValue().getCandidateBucket() + " to " +
                targetBucket + " with gain " + gain);
          }
          vertex.getValue().setCandidateBucket(targetBucket);
        }
      };
    }
  }

}
