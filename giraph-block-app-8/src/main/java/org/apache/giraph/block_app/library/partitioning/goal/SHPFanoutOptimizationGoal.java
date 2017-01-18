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
package org.apache.giraph.block_app.library.partitioning.goal;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.array.ReducerArrayHandle;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.library.partitioning.SHPExecutionStage;
import org.apache.giraph.block_app.library.partitioning.SHPLoggingBuilder;
import org.apache.giraph.block_app.library.partitioning.SocialHashPartitionerSettings;
import org.apache.giraph.block_app.library.partitioning.goal.cache.CachedGeneratedArray;
import org.apache.giraph.block_app.library.partitioning.recursive.MoveBucketLimiter;
import org.apache.giraph.block_app.library.partitioning.recursive.MoveBucketLimiterImpl;
import org.apache.giraph.block_app.library.partitioning.recursive.RecursiveSettings;
import org.apache.giraph.block_app.library.partitioning.vertex.CachedNeighborData;
import org.apache.giraph.block_app.library.partitioning.vertex.SocialHashPartitionerVertexValue;
import org.apache.giraph.block_app.library.partitioning.vertex.SocialHashPartitionerVertexValueImpl;
import org.apache.giraph.block_app.reducers.array.BasicArrayReduce;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.combiner.SumMessageCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.function.Supplier;
import org.apache.giraph.function.primitive.Obj2FloatFunction;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.giraph.types.ops.collections.ResettableIterator;
import org.apache.giraph.types.ops.collections.array.WLongArrayList;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Fanout optimization goal
 */
public class SHPFanoutOptimizationGoal implements SHPOptimizationGoal {
  private static final Logger LOG =
      Logger.getLogger(SHPFanoutOptimizationGoal.class);

  /** Formula for computing goal */
  private interface GoalFormula {
    double comparisonEval(int neighbors, int totalNeighbors);
    double toObjective(double comparisonEval);
    float scaleThreshold(float threshold);
  }

  /**
   * Specialization of goal formulas for when p->0.
   * This is equivelent to clique-net objective.
   */
  private static class GoalFanoutP0 implements GoalFormula {
    @Override
    public double comparisonEval(int n, int t) {
      return n * n * (t > 1 ? (1.0 / (t - 1)) : 1);
    }

    @Override
    public double toObjective(double comparisonEval) {
      return comparisonEval;
    }

    @Override
    public float scaleThreshold(float threshold) {
      return threshold;
    }
  }

  /**
   * Specialization of goal formulas when p=1.
   * This is equivalent to fanout objective.
   */
  private static class GoalFanoutP1 implements GoalFormula {
    @Override
    public double comparisonEval(int n, int t) {
      return n > 0 ? - 1 : 0;
    }

    @Override
    public double toObjective(double comparisonEval) {
      return - comparisonEval;
    }

    @Override
    public float scaleThreshold(float threshold) {
      return threshold;
    }
  }

  /**
   * Specialization of goal formulas when p in (0, 1).
   * This represents p-fanout objective.
   */
  private static class GoalFanoutP implements GoalFormula {
    private final float fanoutProbability;
    private final int splitInto;
    private final CachedGeneratedArray exponentCache;

    public GoalFanoutP(float fanoutProbability, int splitInto) {
      fanoutProbability /= splitInto;
      this.fanoutProbability = fanoutProbability;
      this.splitInto = splitInto;
      exponentCache = new CachedGeneratedArray(
          16000, n -> Math.pow(1 - this.fanoutProbability, n));
    }

    @Override
    public double comparisonEval(int n, int t) {
      return splitInto * exponentCache.apply(n);
    }

    @Override
    public double toObjective(double comparisonEval) {
      return splitInto - comparisonEval;
    }

    @Override
    public float scaleThreshold(float threshold) {
      return threshold;
    }
  }

  private static GoalFormula createFormula(
      float fanoutProbability, int splitInto) {
    if (fanoutProbability == 0) {
      return new GoalFanoutP0();
    } else if (fanoutProbability == 1 && splitInto == 1) {
      return new GoalFanoutP1();
    } else {
      return new GoalFanoutP(fanoutProbability, splitInto);
    }
  }

  @Override
  public Block createMoveBlock(
      GiraphConfiguration conf, Consumer<Boolean> converged) {
    RecursiveSettings recursive =
        SocialHashPartitionerSettings.RECURSIVE.createObject(conf);

    MoveBucketLimiter bucketLimiter = new MoveBucketLimiterImpl();
    ObjectTransfer<Iterable<VertexBucketMessage>> transferMessages =
        new ObjectTransfer<>();

    return new SequenceBlock(
      new SendFromDataToQueryVertices(
          transferMessages, converged,
          SocialHashPartitionerSettings.CONVERGE_MOVE_THRESHOLD.get(conf)),
      new SendFromQueryToDataVertices(
          transferMessages, converged,
          SocialHashPartitionerSettings.USE_FINAL_P_FANOUT.get(conf) ?
          recursive.getFinalNumBuckets() : -1,
          bucketLimiter,
          SocialHashPartitionerSettings.CONVERGE_OBJECTIVE_THRESHOLD.get(conf),
          SocialHashPartitionerSettings.IGNORE_MOVE_GAIN_THRESHOLD.get(conf),
          SocialHashPartitionerSettings.SAMPLING_MOVE_GAIN_THRESHOLD.get(conf))
    );
  }

  @Override
  public Class<? extends Writable> getVertexValueClass(
      GiraphConfiguration conf) {
    return SocialHashPartitionerVertexValueImpl.class;
  }

  /** Piece for sending requests from data to query vertices */
  public static final class SendFromDataToQueryVertices extends
      Piece<LongWritable, SocialHashPartitionerVertexValue, NullWritable,
        VertexBucketMessage, SHPExecutionStage> {
    private final Consumer<Iterable<VertexBucketMessage>> messagesConsumer;
    private final Consumer<Boolean> converged;
    private final float convergeMoveThreshold;
    private ReducerHandle<LongWritable, LongWritable> countVertices;
    private ReducerHandle<LongWritable, LongWritable> countEdges;

    private ReducerHandle<LongWritable, LongWritable> countMoved;
    private ReducerArrayHandle<LongWritable, LongWritable> countMovedTo;

    public SendFromDataToQueryVertices(
        Consumer<Iterable<VertexBucketMessage>> messagesConsumer,
        Consumer<Boolean> converged,
        float convergeMoveThreshold) {
      this.messagesConsumer = messagesConsumer;
      this.converged = converged;
      this.convergeMoveThreshold = convergeMoveThreshold;
    }

    @Override
    public void registerReducers(
        CreateReducersApi reduceApi, SHPExecutionStage executionStage) {
      countVertices = reduceApi.createLocalReducer(SumReduce.LONG);
      countEdges = reduceApi.createLocalReducer(SumReduce.LONG);

      countMoved = reduceApi.createLocalReducer(SumReduce.LONG);
      countMovedTo = BasicArrayReduce.createLocalArrayHandles(
          executionStage.getNumBuckets(),
          LongTypeOps.INSTANCE, SumReduce.LONG, reduceApi);
    }

    @Override
    public
    VertexSender<LongWritable, SocialHashPartitionerVertexValue, NullWritable>
    getVertexSender(
        BlockWorkerSendApi<LongWritable, SocialHashPartitionerVertexValue,
          NullWritable, VertexBucketMessage> workerApi,
        SHPExecutionStage executionStage) {
      VertexBucketMessage message = new VertexBucketMessage();
      return (vertex) -> {
        if (!vertex.getValue().getShouldVertexBeAssignedBucket()) {
          return;
        }

        reduceLong(countVertices, 1);
        reduceLong(countEdges, vertex.getNumEdges());

        int bucket = vertex.getValue().getCandidateBucket();
        if (vertex.getValue().getCurrentBucket() != bucket) {
          if (SocialHashPartitionerSettings.IS_DEBUG) {
            LOG.info(
                "Moved " + vertex.getId() + " from " +
                vertex.getValue().getCurrentBucket() + " to " + bucket);
          }
          reduceLong(countMoved, 1);
          reduceLong(countMovedTo.get(bucket), 1);
          vertex.getValue().setCurrentBucket(bucket);

          message.set(vertex.getId().get(), bucket);
          workerApi.sendMessageToMultipleEdges(
              vertex.getValue().getUsefulEdgeIdIterator(
                  vertex.getEdges().iterator()), message);
        }
      };
    }

    @Override
    public void masterCompute(
        BlockMasterApi masterApi, SHPExecutionStage executionStage) {
      long moved = countMoved.getReducedValue(masterApi).get();
      long total = countVertices.getReducedValue(masterApi).get();
      SHPLoggingBuilder.setCounter(
          "moved vertices", moved, masterApi, executionStage);
      SHPLoggingBuilder sb = new SHPLoggingBuilder();
      sb.appendLine("Moved %d out of %d vertices, total %d edges",
          moved, total, countEdges.getReducedValue(masterApi).get());

      if (SocialHashPartitionerSettings.IS_DEBUG) {
        sb.appendLine("Moved to: " +
          IntStream.range(0, executionStage.getNumBuckets())
            .mapToObj(i -> countMovedTo.get(i).getReducedValue(
                masterApi).toString())
            .collect(Collectors.joining(", ")));
      }

      if (converged != null && moved < convergeMoveThreshold * total) {
        converged.apply(true);
        sb.appendLine("Converged!");
      }
      sb.logToCommandLine(masterApi);
    }

    @Override
    public
    VertexReceiver<LongWritable, SocialHashPartitionerVertexValue,
    NullWritable, VertexBucketMessage>
    getVertexReceiver(
        BlockWorkerReceiveApi<LongWritable> workerApi,
        SHPExecutionStage executionStage) {
      return (vertex, messages) -> {
        messagesConsumer.apply(messages);
      };
    }

    @Override
    protected Class<VertexBucketMessage> getMessageClass() {
      return VertexBucketMessage.class;
    }

    @Override
    protected boolean allowOneMessageToManyIdsEncoding() {
      return true;
    }
  }

  /** Piece for sending back replies from query to data vertices */
  public static final class SendFromQueryToDataVertices extends
      Piece<LongWritable, SocialHashPartitionerVertexValue, NullWritable,
      FloatWritable, SHPExecutionStage> {
    private final Supplier<Iterable<VertexBucketMessage>> messagesSupplier;
    private final Consumer<Boolean> converged;
    private final int finalNumBuckets;
    private final MoveBucketLimiter bucketLimiter;
    private final float convergeObjectiveThreshold;
    private final float ignoreMoveGainThreshold;
    private final float samplingMoveGainThreshold;

    private double previousAffectableObjective = Double.POSITIVE_INFINITY;

    private ReducerHandle<LongWritable, LongWritable> numQueryVertices;
    private ReducerHandle<LongWritable, LongWritable> sumFanout;
    private ReducerHandle<LongWritable, LongWritable> sumAffectableFanout;
    private ReducerHandle<DoubleWritable, DoubleWritable> sumObjective;
    private
    ReducerHandle<DoubleWritable, DoubleWritable> sumAffectableObjective;
    private ReducerHandle<LongWritable, LongWritable> msgsSent;
    private ReducerHandle<LongWritable, LongWritable> msgGroupsSent;
    private ReducerHandle<LongWritable, LongWritable> msgsSampled;
    private ReducerHandle<LongWritable, LongWritable> msgsSkipped;

    public SendFromQueryToDataVertices(
        Supplier<Iterable<VertexBucketMessage>> messagesSupplier,
        Consumer<Boolean> converged,
        int finalNumBuckets, MoveBucketLimiter bucketLimiter,
        float convergeObjectiveThreshold,
        float ignoreMoveGainThreshold, float samplingMoveGainThreshold) {
      this.messagesSupplier = messagesSupplier;
      this.converged = converged;
      this.finalNumBuckets = finalNumBuckets;
      this.bucketLimiter = bucketLimiter;
      this.convergeObjectiveThreshold = convergeObjectiveThreshold;
      this.ignoreMoveGainThreshold = ignoreMoveGainThreshold;
      this.samplingMoveGainThreshold = samplingMoveGainThreshold;
    }

    @Override
    public void registerReducers(
        CreateReducersApi reduceApi, SHPExecutionStage executionStage) {
      numQueryVertices = reduceApi.createLocalReducer(SumReduce.LONG);
      sumFanout = reduceApi.createLocalReducer(SumReduce.LONG);
      sumAffectableFanout = reduceApi.createLocalReducer(SumReduce.LONG);
      sumObjective = reduceApi.createLocalReducer(SumReduce.DOUBLE);
      sumAffectableObjective = reduceApi.createLocalReducer(SumReduce.DOUBLE);

      msgsSent = reduceApi.createLocalReducer(SumReduce.LONG);
      msgGroupsSent = reduceApi.createLocalReducer(SumReduce.LONG);
      msgsSampled = reduceApi.createLocalReducer(SumReduce.LONG);
      msgsSkipped = reduceApi.createLocalReducer(SumReduce.LONG);
    }

    @Override
    public
    VertexSender<LongWritable, SocialHashPartitionerVertexValue, NullWritable>
    getVertexSender(
        BlockWorkerSendApi<LongWritable, SocialHashPartitionerVertexValue,
          NullWritable, FloatWritable> workerApi,
        SHPExecutionStage executionStage) {
      WLongArrayList targets = new WLongArrayList();
      ResettableIterator<LongWritable> targetsIter = targets.fastIteratorW();
      FloatWritable messageValue = new FloatWritable();

      Obj2FloatFunction<SHPExecutionStage> fanoutProbabilityF =
          SocialHashPartitionerSettings.FANOUT_PROBABILITY.createObject(
              workerApi.getConf());

      GoalFormula formula = createFormula(
          fanoutProbabilityF.apply(executionStage),
          finalNumBuckets <= 0 ? 1 :
          (finalNumBuckets / executionStage.getNumBuckets()));

      double isolatedObjective =
          formula.toObjective(formula.comparisonEval(0, 1)) +
          formula.toObjective(formula.comparisonEval(1, 1));

      return (vertex) -> {
        Iterable<VertexBucketMessage> messages = messagesSupplier.get();

        CachedNeighborData neighbors =
            vertex.getValue().updateNeighborData(messages.iterator());

        long fanout = vertex.getValue().getRemovedIsolatedNeighbors();
        double objective =
            vertex.getValue().getRemovedIsolatedNeighbors() *
            isolatedObjective;
        long affectableFanout = 0;
        double affectableObjective = 0;

        boolean isQuery = vertex.getValue().getRemovedIsolatedNeighbors() > 0;
        // check if it's a query vertex
        if (neighbors != null) {
          isQuery = true;
          int start = 0;
          while (start < neighbors.size()) {
            int curBucket = neighbors.getBucket(start);

            int leftBucket = bucketLimiter.startBucket(curBucket);
            int rightBucket = bucketLimiter.endBucket(curBucket);

            int mid = start;
            while (mid < neighbors.size() &&
                neighbors.getBucket(mid) == leftBucket) {
              mid++;
            }

            int end = mid;
            while (end < neighbors.size() &&
                neighbors.getBucket(end) == rightBucket) {
              end++;
            }

            int totalInPair = end - start;
            double evalLeft = formula.comparisonEval(mid - start, totalInPair);
            double evalRight = formula.comparisonEval(end - mid, totalInPair);
            double curEval = evalLeft + evalRight;
            double curObjective =
                formula.toObjective(evalLeft) + formula.toObjective(evalRight);
            objective += curObjective;
            double curFanout = (start < mid ? 1 : 0) + (mid < end ? 1 : 0);
            fanout += curFanout;

            if (end - start > 1) {
              affectableObjective += curObjective - (
                  formula.toObjective(
                      formula.comparisonEval(end - start, totalInPair)) +
                  formula.toObjective(
                      formula.comparisonEval(0, end - start)));
              affectableFanout += curFanout - 1;

              sendGainsIfNeeded(
                  start, mid, end - mid, curEval, targets, targetsIter,
                  messageValue, neighbors, workerApi, formula);

              sendGainsIfNeeded(
                  mid, end, mid - start, curEval, targets, targetsIter,
                  messageValue, neighbors, workerApi, formula);
            }
            start = end;
          }
        }

        if (isQuery) {
          reduceLong(numQueryVertices, 1);
          reduceLong(sumFanout, fanout);
          reduceLong(sumAffectableFanout, affectableFanout);
          reduceDouble(sumObjective, objective);
          reduceDouble(sumAffectableObjective, affectableObjective);
        }
      };
    }

    // CHECKSTYLE: stop ParameterNumber
    private void sendGainsIfNeeded(
        int rangeStart, int rangeEnd, int otherSize, double oldObjective,
        WLongArrayList targets, ResettableIterator<LongWritable> targetsIter,
        FloatWritable messageValue, CachedNeighborData neighbors,
        BlockWorkerSendApi<LongWritable, SocialHashPartitionerVertexValue,
          NullWritable, FloatWritable> workerApi,
        GoalFormula formula) {
     // CHECKSTYLE: resume ParameterNumber
      int thisSize = rangeEnd - rangeStart;
      if (0 < thisSize && otherSize + 1 != thisSize) {
        double newObjective =
            formula.comparisonEval(otherSize + 1, thisSize + otherSize) +
            formula.comparisonEval(thisSize - 1, thisSize + otherSize);
        double gain = newObjective - oldObjective;

        boolean toSend = false;
        if (Math.abs(gain) >
            formula.scaleThreshold(samplingMoveGainThreshold)) {
          toSend = true;
        } else if (Math.abs(gain) >
            formula.scaleThreshold(ignoreMoveGainThreshold)) {
          reduceLong(msgsSampled, thisSize);
          if (ThreadLocalRandom.current().nextFloat() <
              SocialHashPartitionerSettings.SAMPLING_MOVE_GAIN_RATIO) {
            toSend = true;
            gain /= SocialHashPartitionerSettings.SAMPLING_MOVE_GAIN_RATIO;
          }
        }

        if (toSend) {
          messageValue.set((float) gain);
          neighbors.fillIdSubset(targets, rangeStart, rangeEnd);
          targetsIter.reset();

          reduceLong(msgGroupsSent, 1);
          reduceLong(msgsSent, thisSize);
          workerApi.sendMessageToMultipleEdges(targetsIter, messageValue);
        } else if (gain != 0) {
          reduceLong(msgsSkipped, thisSize);
        }
      }
    }

    @Override
    public void masterCompute(
        BlockMasterApi masterApi, SHPExecutionStage executionStage) {
      long numQueries = numQueryVertices.getReducedValue(masterApi).get();
      double fanout =
          ((double) sumFanout.getReducedValue(masterApi).get()) / numQueries;
      double affectableFanout =
          ((double) sumAffectableFanout.getReducedValue(masterApi).get()) /
          numQueries;
      double objective =
          sumObjective.getReducedValue(masterApi).get() / numQueries;
      double affectableObjective =
          sumAffectableObjective.getReducedValue(masterApi).get() / numQueries;

      SHPLoggingBuilder.setCounter(
          "fanout", fanout, masterApi, executionStage);
      SHPLoggingBuilder.setCounter(
          "objective", objective, masterApi, executionStage);

      SHPLoggingBuilder sb = new SHPLoggingBuilder();
      sb.appendLine("At: " + executionStage);
      sb.appendLine("Num query vertices: " + numQueries);
      sb.appendLine(
          "Avg fanout: " + fanout + ", caused by current split: " +
          affectableFanout);
      sb.appendLine(
          "Avg objective: " + objective + ", caused by current split: " +
          affectableObjective);

      long sent = msgsSent.getReducedValue(masterApi).get();
      long groupsSent = msgGroupsSent.getReducedValue(masterApi).get();
      long skipped = msgsSkipped.getReducedValue(masterApi).get();
      double total = sent + skipped;
      sb.appendLine(
          "Msgs sent: %d, avg per group %.2f, ratio msgs skipped %.5f, ratio " +
          "msgs sampled %.5f",
          sent, ((double) sent) / groupsSent,
          skipped / total,
          msgsSampled.getReducedValue(masterApi).get() / total);

      if (converged != null && convergeObjectiveThreshold > 0 &&
          Math.abs(affectableObjective - previousAffectableObjective) <
          convergeObjectiveThreshold *
          Math.max(objective, previousAffectableObjective)) {
        converged.apply(true);
        sb.appendLine("Converged!");
      }
      sb.logToCommandLine(masterApi);
      previousAffectableObjective = affectableObjective;
    }

    @Override
    public
    VertexReceiver<LongWritable, SocialHashPartitionerVertexValue,
    NullWritable, FloatWritable> getVertexReceiver(
        BlockWorkerReceiveApi<LongWritable> workerApi,
        SHPExecutionStage executionStage) {
      return (vertex, messages) -> {
        Iterator<FloatWritable> iter = messages.iterator();
        if (iter.hasNext()) {
          vertex.getValue().setBisectionMoveGain(iter.next().get());
          if (SocialHashPartitionerSettings.IS_DEBUG) {
            LOG.info(
                vertex.getId() + " has gain " +
            vertex.getValue().getBisectionMoveGain());
          }
          Preconditions.checkState(!iter.hasNext());
        } else {
          vertex.getValue().setBisectionMoveGain(0);
        }
      };
    }

    @Override
    protected
    MessageCombiner<WritableComparable, FloatWritable> getMessageCombiner(
        ImmutableClassesGiraphConfiguration conf) {
      return SumMessageCombiner.FLOAT;
    }

    @Override
    protected boolean allowOneMessageToManyIdsEncoding() {
      return true;
    }
  }
}
