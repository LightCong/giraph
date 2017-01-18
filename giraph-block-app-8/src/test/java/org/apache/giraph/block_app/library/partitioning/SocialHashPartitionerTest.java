package org.apache.giraph.block_app.library.partitioning;

import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.block_app.library.partitioning.vertex.SocialHashPartitionerVertexValue;
import org.apache.giraph.block_app.test_setup.TestGraphUtils;
import org.apache.giraph.block_app.test_setup.graphs.SyntheticGraphInit;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

public class SocialHashPartitionerTest {
  private void testSumOverSameGroup(boolean fullGiraphEnv) throws Exception {
    TestGraphUtils.runTest(
//        new SmallDirectedTreeGraphInit<LongWritable, SocialHashPartitionerVertexValue, NullWritable>(),
        new SyntheticGraphInit<LongWritable, SocialHashPartitionerVertexValue, NullWritable>(),
        (graph) -> {
          for (Vertex<LongWritable, SocialHashPartitionerVertexValue, NullWritable> vtx : graph.getTestGraph()) {
          }
        },
        (GiraphConfiguration conf) -> {
          BlockUtils.setBlockFactoryClass(conf, SocialHashPartitionerBlockFactory.class);
          TestGraphUtils.USE_FULL_GIRAPH_ENV_IN_TESTS.set(conf, fullGiraphEnv);

          SocialHashPartitionerSettings.RECURSIVE.setCodeSnippet(conf, "numSplits=6;");
          SocialHashPartitionerSettings.NUM_ITERATIONS_PER_SPLIT.set(conf, 40);
          SocialHashPartitionerSettings.FANOUT_PROBABILITY.setCodeSnippet(
              conf, "0.5f");
          SocialHashPartitionerSettings.ALLOWED_IMBALANCE.set(conf, 0.05f);

          SyntheticGraphInit.NUM_VERTICES.set(conf, 50000);
          SyntheticGraphInit.NUM_EDGES_PER_VERTEX.set(conf, 10);
          SyntheticGraphInit.NUM_COMMUNITIES.set(conf, 100);
          SyntheticGraphInit.ACTUAL_LOCALITY_RATIO.set(conf, 0.8f);
        });
  }

  @Test
  public void testWithLocalBlockRunner() throws Exception {
    testSumOverSameGroup(false);
  }

  @Test
  public void testWithGiraphEnv() throws Exception {
    testSumOverSameGroup(true);
  }
}
