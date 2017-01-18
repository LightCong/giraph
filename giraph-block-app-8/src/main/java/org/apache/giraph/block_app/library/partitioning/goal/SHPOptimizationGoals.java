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

import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.function.Consumer;
import org.apache.hadoop.io.Writable;

/** List of available optimization goals */
public enum SHPOptimizationGoals implements SHPOptimizationGoal {
  FANOUT(new SHPFanoutOptimizationGoal());

  private final SHPOptimizationGoal optimizationGoal;

  private SHPOptimizationGoals(SHPOptimizationGoal optimizationGoal) {
    this.optimizationGoal = optimizationGoal;
  }

  @Override
  public Class<? extends Writable> getVertexValueClass(
      GiraphConfiguration conf) {
    return optimizationGoal.getVertexValueClass(conf);
  }

  @Override
  public Block createMoveBlock(
      GiraphConfiguration conf, Consumer<Boolean> converged) {
    return optimizationGoal.createMoveBlock(conf, converged);
  }
}
