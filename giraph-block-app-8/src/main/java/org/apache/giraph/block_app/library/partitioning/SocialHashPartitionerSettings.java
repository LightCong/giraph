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

import org.apache.giraph.block_app.library.partitioning.goal.SHPOptimizationGoals;
import org.apache.giraph.block_app.library.partitioning.recursive.RecursiveSettings;
import org.apache.giraph.compiling.LambdaConfOption;
import org.apache.giraph.compiling.ObjectInitializerConfOption;
import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.EnumConfOption;
import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.function.primitive.Obj2FloatFunction;

import com.google.common.reflect.TypeToken;

/** All configurable parameters for Social Hash Partitioner */
public class SocialHashPartitionerSettings {
  public static final ObjectInitializerConfOption<RecursiveSettings>
  RECURSIVE = new ObjectInitializerConfOption<>(
        "social_hash_partitioner.recursive", RecursiveSettings.class,
        "", "Recursive settings to use");

  public static final IntConfOption
  NUM_ITERATIONS_PER_SPLIT = new IntConfOption(
      "social_hash_partitioner.num_iterations_per_split", 50,
      "Num iterations");

  public static final StrConfOption
  ASSIGN_WITH = new StrConfOption(
      "social_hash_partitioner.assign_with", "Random",
      "Which bucket assigner to use, when vertex has no initial bucket in " +
      "input. Current choices are: Random, AllTo0, MinHash");

  public static final BooleanConfOption
  KEEP_STANDALONE_VERTICES = new BooleanConfOption(
      "social_hash_partitioner.keep_standalone_vertices", true, "");

  public static final EnumConfOption<SHPOptimizationGoals>
  OPTIMIZE_FOR = new EnumConfOption<>(
      "social_hash_partitioner.optimize_for",
      SHPOptimizationGoals.class, SHPOptimizationGoals.FANOUT,
      "Optimization goal (objective function) name. Choices are in " +
      "SHPOptimizationGoals enum.");

  public static final LambdaConfOption<Obj2FloatFunction<SHPExecutionStage>>
  FANOUT_PROBABILITY = new LambdaConfOption<>(
      "social_hash_partitioner.fanout.fanout_probability",
      new TypeToken<Obj2FloatFunction<SHPExecutionStage>>() { },
      "0.5f",
      "smoothing factor used for fanout, between 0.0f and 1.0f",
      "stage");

  public static final BooleanConfOption
  USE_FINAL_P_FANOUT = new BooleanConfOption(
      "social_hash_partitioner.fanout.use_final_p_fanout", true,
      "");

  public static final FloatConfOption
  ALLOWED_IMBALANCE = new FloatConfOption(
      "social_hash_partitioner.allowed_imbalance", 0.0f,
      "allowed imbalance between buckets, between 0.0f and 1.0f");

  public static final FloatConfOption
  CONVERGE_MOVE_THRESHOLD = new FloatConfOption(
      "social_hash_partitioner.converge.move_threshold", 0.0001f, "");

  public static final FloatConfOption
  CONVERGE_OBJECTIVE_THRESHOLD = new FloatConfOption(
      "social_hash_partitioner.converge.objective_threshold", 0f, "");

  public static final FloatConfOption
  MOVE_PROBABILITY = new FloatConfOption(
      "social_hash_partitioner.decide.move_probability", 0.8f, "");

  public static final FloatConfOption
  MAX_MOVE_RATIO = new FloatConfOption(
      "social_hash_partitioner.decide.max_move_ratio", 0.1f, "");

  public static final IntConfOption
  GAIN_HISTOGRAM_NUM_QUARTER_BINS = new IntConfOption(
      "social_hash_partitioner.decide.gain_histogram_num_quarter_bins", 30,
      "");

  public static final FloatConfOption
  GAIN_HISTOGRAM_EXPONENT = new FloatConfOption(
      "social_hash_partitioner.decide.gain_histogram_exponent",
      (float) Math.pow(2, 0.5), "");

  public static final FloatConfOption
  IGNORE_MOVE_GAIN_THRESHOLD = new FloatConfOption(
      "social_hash_partitioner.move.ignore_gain_threshold", 1e-8f, "");

  public static final FloatConfOption
  SAMPLING_MOVE_GAIN_THRESHOLD = new FloatConfOption(
      "social_hash_partitioner.move.sampling_gain_threshold", 1e-3f, "");

  public static final float SAMPLING_MOVE_GAIN_RATIO = 0.25f;

  public static final boolean IS_DEBUG = false;

  private SocialHashPartitionerSettings() { }
}
