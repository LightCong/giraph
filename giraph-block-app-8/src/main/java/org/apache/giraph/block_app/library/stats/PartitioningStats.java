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
package org.apache.giraph.block_app.library.stats;

import java.util.stream.LongStream;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.block_app.library.SendMessageChain;
import org.apache.giraph.block_app.reducers.array.BasicArrayReduce;
import org.apache.giraph.function.primitive.DoubleConsumer;
import org.apache.giraph.function.primitive.PrimitiveRefs.IntRef;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.reducers.impl.PairReduce;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.giraph.types.ops.collections.array.WArrayList;
import org.apache.giraph.types.ops.collections.array.WLongArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

/**
 * Utility blocks for calculating stats for a given partitioning - an
 * assignment of vertices to buckets.
 */
public class PartitioningStats {
  private static final Logger LOG = Logger.getLogger(DirectedGraphStats.class);

  private PartitioningStats() { }

  /**
   * Calculate edge locality - ratio of edges that are within a same bucket.
   */
  public static <V extends Writable> Block calculateEdgeLocality(
      SupplierFromVertex<WritableComparable, V, Writable, LongWritable>
        bucketSupplier,
      DoubleConsumer edgeLocalityConsumer) {
    final Pair<LongWritable, LongWritable> pair =
        Pair.of(new LongWritable(), new LongWritable());
    return SendMessageChain.<WritableComparable, V, Writable, LongWritable>
    startSendToNeighbors(
        "CalcLocalEdgesPiece",
        LongWritable.class,
        bucketSupplier
    ).endReduceWithMaster(
        "AggregateEdgeLocalityPiece",
        new PairReduce<>(SumReduce.LONG, SumReduce.LONG),
        (vertex, messages) -> {
          long bucket = bucketSupplier.get(vertex).get();
          int local = 0;
          int total = 0;
          for (LongWritable otherCluster : messages) {
            total++;
            if (bucket == otherCluster.get()) {
              local++;
            }
          }
          pair.getLeft().set(local);
          pair.getRight().set(total);
          return pair;
        },
        (reducedPair, master) -> {
          long localEdges = reducedPair.getLeft().get();
          long totalEdges = reducedPair.getRight().get();
          double edgeLocality = (double) localEdges / totalEdges;
          master.logToCommandLine("locality ratio = " + edgeLocality);
          master.getCounter(
              "Partitioning stats", "edge locality (in percent * 1000)")
            .setValue((long) (edgeLocality * 100000));
          if (edgeLocalityConsumer != null) {
            edgeLocalityConsumer.apply(edgeLocality);
          }
        }
    );
  }

  /**
   * Calculates average fanout - average number of distinct buckets that vertex
   * has neighbors in.
   */
  public static <V extends Writable> Block calculateFanout(
      SupplierFromVertex<WritableComparable, V, Writable, LongWritable>
        bucketSupplier,
      DoubleConsumer averageFanoutConsumer) {
    final Pair<LongWritable, LongWritable> zero =
        Pair.of(new LongWritable(0), new LongWritable(0));
    final Pair<LongWritable, LongWritable> pair =
        Pair.of(new LongWritable(), new LongWritable(1));
    return SendMessageChain.<WritableComparable, V, Writable, LongWritable>
    startSendToNeighbors(
        "CalcFanoutPiece",
        LongWritable.class,
        bucketSupplier
    ).endReduceWithMaster(
        "AggregateFanoutPiece",
        new PairReduce<>(SumReduce.LONG, SumReduce.LONG),
        (vertex, messages) -> {
          LongSet setOfNeighborBuckets = new LongOpenHashSet();
          for (LongWritable neighborBucket : messages) {
            setOfNeighborBuckets.add(neighborBucket.get());
          }
          if (setOfNeighborBuckets.isEmpty()) {
            return zero;
          } else {
            pair.getLeft().set(setOfNeighborBuckets.size());
            return pair;
          }
        },
        (reducedPair, master) -> {
          long fanout = reducedPair.getLeft().get();
          long numVertices = reducedPair.getRight().get();
          double avgFanout = (double) fanout / numVertices;
          master.logToCommandLine("fanout ratio = " + avgFanout);
          master.getCounter("Partitioning stats", "fanout (* 1000)")
            .setValue((long) (avgFanout * 1000));
          if (averageFanoutConsumer != null) {
            averageFanoutConsumer.apply(avgFanout);
          }
        }
    );
  }

  public static <V extends Writable> Block calculateImbalance(
      int numBuckets,
      SupplierFromVertex<WritableComparable, V, Writable, LongWritable>
        bucketSupplier,
      DoubleConsumer imbalanceConsumer) {
    return Pieces.<Pair<IntRef, LongWritable>, WArrayList<LongWritable>,
          WritableComparable, V, Writable>reduceWithMaster(
        "CalcBalancePiece",
        new BasicArrayReduce<>(
            numBuckets, LongTypeOps.INSTANCE, SumReduce.LONG),
        (vertex) -> {
          LongWritable bucket = bucketSupplier.get(vertex);
          if (bucket != null) {
            Preconditions.checkState(
                bucket.get() >= 0 && bucket.get() < numBuckets);
            return ImmutablePair.of(
                new IntRef((int) bucket.get()), new LongWritable(1));
          }
          return null;
        },
        (value, master) -> {
          long sum = LongStream.of(
              ((WLongArrayList) value).toLongArray()).sum();
          long max = LongStream.of(
              ((WLongArrayList) value).toLongArray()).max().getAsLong();
          double imbalance = max / (((double) sum) / numBuckets);

          master.logToCommandLine(
              "imbalance ratio = " + imbalance + " = " + max + " / " + sum +
              " / " + numBuckets);
          master.getCounter("Partitioning stats", "imbalance (* 1000)")
            .setValue((long) (imbalance * 1000));

          if (imbalanceConsumer != null) {
            imbalanceConsumer.apply(imbalance);
          }
        });

  }
}
