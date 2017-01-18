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
package org.apache.giraph.block_app.library.partitioning.confs;

import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.block_app.library.partitioning.SocialHashPartitionerBlockFactory;
import org.apache.giraph.block_app.library.partitioning.SocialHashPartitionerSettings;
import org.apache.giraph.conf.BulkConfigurator;
import org.apache.giraph.conf.GiraphConfiguration;

/** Configuration used for experiments in the corresponding paper */
public class SHPPaperConfs implements BulkConfigurator {

  @Override
  public void configure(GiraphConfiguration conf) {
    BlockUtils.setBlockFactoryClass(
        conf, SocialHashPartitionerBlockFactory.class);

    SocialHashPartitionerSettings.MAX_MOVE_RATIO.setIfUnset(conf, 0.1f);
    SocialHashPartitionerSettings.IGNORE_MOVE_GAIN_THRESHOLD.setIfUnset(
        conf, 0);
    SocialHashPartitionerSettings.SAMPLING_MOVE_GAIN_THRESHOLD.setIfUnset(
        conf, 0.001f);
    SocialHashPartitionerSettings.CONVERGE_OBJECTIVE_THRESHOLD.setIfUnset(
        conf, 0);
    SocialHashPartitionerSettings.CONVERGE_MOVE_THRESHOLD.setIfUnset(conf, 0);
    SocialHashPartitionerSettings.ALLOWED_IMBALANCE.setIfUnset(conf, 0.045f);
    SocialHashPartitionerSettings.GAIN_HISTOGRAM_EXPONENT.setIfUnset(
        conf, (float) Math.pow(2, 0.5));
    SocialHashPartitionerSettings.USE_FINAL_P_FANOUT.setIfUnset(conf, true);
    SocialHashPartitionerSettings.MOVE_PROBABILITY.setIfUnset(conf, 0.8f);

    SocialHashPartitionerSettings.NUM_ITERATIONS_PER_SPLIT.setIfUnset(
        conf, 20);
    SocialHashPartitionerSettings.FANOUT_PROBABILITY.setCodeSnippetIfUnset(
        conf, "0.5f");
  }
}
