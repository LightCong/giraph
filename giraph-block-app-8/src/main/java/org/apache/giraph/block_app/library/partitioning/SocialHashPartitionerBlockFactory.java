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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.giraph.block_app.framework.AbstractBlockFactory;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.EmptyBlock;
import org.apache.giraph.block_app.framework.block.RepeatBlock;
import org.apache.giraph.block_app.framework.block.RepeatUntilBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.block_app.library.VertexSuppliers;
import org.apache.giraph.block_app.library.iteration.IterationCounterPiece;
import org.apache.giraph.block_app.library.partitioning.assignment.AllToSameBucketAssigner;
import org.apache.giraph.block_app.library.partitioning.assignment.BucketAssigner;
import org.apache.giraph.block_app.library.partitioning.assignment.RandomBucketAssigner;
import org.apache.giraph.block_app.library.partitioning.assignment.SHPInitializePiece;
import org.apache.giraph.block_app.library.partitioning.confs.SHPPaperConfs;
import org.apache.giraph.block_app.library.partitioning.decide.SHPDecideUtils;
import org.apache.giraph.block_app.library.partitioning.recursive.RecursiveSettings;
import org.apache.giraph.block_app.library.partitioning.recursive.SHPSplitPiece;
import org.apache.giraph.block_app.library.partitioning.vertex.CachedNeighborData;
import org.apache.giraph.block_app.library.partitioning.vertex.SocialHashPartitionerVertexValue;
import org.apache.giraph.block_app.library.prepare_graph.PrepareGraphPieces;
import org.apache.giraph.block_app.library.stats.PartitioningStats;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.Function;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.giraph.types.ops.collections.ResettableIterator;
import org.apache.giraph.types.ops.collections.array.WLongArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Preconditions;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

/** Main entry point to Social Hash Partitioner */
public class SocialHashPartitionerBlockFactory
    extends AbstractBlockFactory<SHPExecutionStage> {

  @Override
  public Block createBlock(GiraphConfiguration conf) {
    Block prepareGraphBlock = new SequenceBlock(
        SocialHashPartitionerSettings.KEEP_STANDALONE_VERTICES.get(conf) ?
          new EmptyBlock() : PrepareGraphPieces.removeStandAloneVertices(),
        PrepareGraphPieces.removeDuplicateEdges(LongTypeOps.INSTANCE),
        Pieces.<LongWritable, SocialHashPartitionerVertexValue, Writable>
          forAllVertices(
            "mark_as_data",
            vertex -> {
              vertex.getValue().setShouldVertexBeAssignedBucket(true);
              vertex.getValue().initUselessEdgesSet(vertex.getNumEdges());
            })
    );

    // initialize
    RecursiveSettings recursive =
        SocialHashPartitionerSettings.RECURSIVE.createObject(conf);
    Block initializeBlock = createInitializeBlock(conf, recursive);

    // iterations
    int numIterationsPerSplit =
        SocialHashPartitionerSettings.NUM_ITERATIONS_PER_SPLIT.get(conf);
    Block iterationsBlock = createSplittingIterationsBlock(
      conf,
      numIterationsPerSplit,
      recursive
    );

    return new SequenceBlock(
      prepareGraphBlock,
      initializeBlock,
      iterationsBlock,
      calcEndStatsBlock(conf, recursive)
    );
  }

  private Block calcEndStatsBlock(
      GiraphConfiguration conf, RecursiveSettings recursive) {
    SupplierFromVertex<WritableComparable, SocialHashPartitionerVertexValue,
    Writable, LongWritable> bucketSupplier =
      vertex -> {
        if (vertex.getValue().getShouldVertexBeAssignedBucket()) {
          if (vertex.getValue().getCurrentBucket() !=
              SocialHashPartitionerVertexValue.BUCKET_NOT_SET) {
            return new LongWritable(vertex.getValue().getCurrentBucket());
          }
        }
        return null;
      };
    return new SequenceBlock(
      PartitioningStats.calculateFanout(bucketSupplier, null),
      PartitioningStats.calculateImbalance(
          recursive.getFinalNumBuckets(), bucketSupplier, null)
    );
  }

  protected static Block createInitializeBlock(
      GiraphConfiguration conf, RecursiveSettings recursive) {
    return moveAfterBlock(
        createBucketAssigner(
            conf,
            (bucketAssigner) -> SHPInitializePiece.create(
                recursive.getInitialNumBuckets(), bucketAssigner)
        ),
        conf,
        null
    );
  }

  public static Block moveAfterBlock(
      Block block, GiraphConfiguration conf, Consumer<Boolean> converged) {
    return new SequenceBlock(
        block,
        SocialHashPartitionerSettings.OPTIMIZE_FOR.get(conf).createMoveBlock(
            conf, converged)
    );
  }

  public static <V extends SocialHashPartitionerVertexValue>
  Block createBucketAssigner(
      Configuration conf,
      Function<BucketAssigner<V>, Block> assignBlock
  ) {
    String assignWith = SocialHashPartitionerSettings.ASSIGN_WITH.get(conf);
    switch (assignWith) {
    case "Random":
      return assignBlock.apply(new RandomBucketAssigner<>());
    case "AllToSame":
      return assignBlock.apply(new AllToSameBucketAssigner<>());
//    case "MinHash":
//      return MinHashInitializer.createAssignBlock(assignBlock, conf);
//    case "BFS":
//      return BFSInitializerForBalancedPartitioning.createAssignBlock(
//          assignBlock, conf, SocialHashSettings.getNumFinalBuckets(conf));
    default:
      throw new IllegalArgumentException(
          "Unknown bucket assigner option specified - " + assignWith);
    }
  }

  /**
   * Factory which creates set of iterations of balance partitioning,
   * interleaved with set of splits.
   */
  public static Block createSplittingIterationsBlock(
    GiraphConfiguration conf,
    int numIterationsPerSplit,
    RecursiveSettings recursiveSettings
  ) {
    int numSplits = recursiveSettings.getNumSplits();

    List<Block> iterationsBlocks = new ArrayList<>();
    if (recursiveSettings.runIterationsBeforeFirstSplit()) {

      iterationsBlocks.add(
        createIterationsBlock(conf, numIterationsPerSplit, recursiveSettings)
      );
    }

    if (numSplits > 0) {
      Preconditions.checkState(recursiveSettings.isRecursiveSplitting());
      iterationsBlocks.add(new RepeatBlock(
        numSplits,
        new SequenceBlock(
          markUselessEdges(),
          moveAfterBlock(
            createBucketAssigner(
              conf,
              (bucketAssigner) -> new SHPSplitPiece<>(
                  recursiveSettings, bucketAssigner)),
            conf,
            null),
          createIterationsBlock(
            conf,
            numIterationsPerSplit,
            recursiveSettings)
        )
      ));
    }
    return new SequenceBlock(iterationsBlocks);
  }

  /**
   * Creates set of iterations of balance partitioning
   */
  public static Block createIterationsBlock(
      GiraphConfiguration conf, int numIterations,
      RecursiveSettings recursiveSettings) {
    ObjectTransfer<Boolean> converged = new ObjectTransfer<>();
    Block iterationsBlock = new SequenceBlock(
      new RepeatUntilBlock(
        numIterations,
        new SequenceBlock(
            moveAfterBlock(
                SHPDecideUtils.createDecideBlock(conf, recursiveSettings),
                conf, converged),
            new IterationCounterPiece()
        ),
        converged
      )
    );

    return iterationsBlock;
  }

  public static Block markUselessEdges() {
    WLongArrayList targets = new WLongArrayList();
    ResettableIterator<LongWritable> targetsIter = targets.fastIteratorW();
    return Pieces.<LongWritable, SocialHashPartitionerVertexValue,
        NullWritable, LongWritable>
    sendMessage(
        "MarkUselessEdges",
        LongWritable.class,
        VertexSuppliers.vertexIdSupplier(),
        (vertex) -> {
          targets.clear();
          CachedNeighborData neighbors =
              vertex.getValue().updateNeighborData(
                  Collections.emptyIterator());
          if (neighbors != null) {
            int start = 0;
            while (start < neighbors.size()) {
              int end = start + 1;
              while (end < neighbors.size() &&
                   neighbors.getBucket(start) == neighbors.getBucket(end)) {
                end++;
              }
              if (end == start + 1) {
                targets.add(neighbors.getId(start));
              }
              start = end;
            }
            vertex.getValue().incrementRemovedIsolatedNeighbors(
                targets.size());
            targetsIter.reset();
            return targetsIter;
          } else {
            return null;
          }
        },
        (vertex, messages) -> {
          LongOpenHashSet toRemove = new LongOpenHashSet();
          for (LongWritable message : messages) {
            toRemove.add(message.get());
          }
          int index = 0;
          for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
            if (toRemove.contains(edge.getTargetVertexId().get())) {
              vertex.getValue().markEdgeAsUseless(index);
            }
            index++;
          }
        });
  }

  @Override
  public SHPExecutionStage createExecutionStage(GiraphConfiguration conf) {
    return new SHPExecutionStageImpl();
  }

  @Override
  protected Class<? extends WritableComparable> getVertexIDClass(
      GiraphConfiguration conf) {
    return LongWritable.class;
  }

  @Override
  protected Class<? extends Writable> getVertexValueClass(
      GiraphConfiguration conf) {
    return SocialHashPartitionerSettings.OPTIMIZE_FOR.get(conf)
        .getVertexValueClass(conf);
  }

  @Override
  protected Class<? extends Writable> getEdgeValueClass(
      GiraphConfiguration conf) {
    return NullWritable.class;
  }

  @Override
  protected void additionalInitConfig(GiraphConfiguration conf) {
    GiraphConstants.RESOLVER_CREATE_VERTEX_ON_MSGS.setIfUnset(conf, true);
  }

  @Override
  protected String[] getConvenienceConfiguratorPackages() {
    return new String[] { SHPPaperConfs.class.getPackage().getName() };
  }
}
