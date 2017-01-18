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

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.library.stats.DirectedGraphStats;
import org.apache.log4j.Logger;

/** Logging utility */
public class SHPLoggingBuilder {
  private static final Logger LOG = Logger.getLogger(DirectedGraphStats.class);

  private final StringBuilder sb = new StringBuilder();

  public SHPLoggingBuilder appendLine(String line) {
    sb.append("\nM> " + line);
    return this;
  }

  public SHPLoggingBuilder appendLine(String format, Object... args) {
    appendLine(String.format(format, args));
    return this;
  }

  public void logToCommandLine(BlockMasterApi masterApi) {
    LOG.info(sb);
    masterApi.logToCommandLine(sb.toString());
  }

  public static void setCounter(
      String counterName, long value,
      BlockMasterApi masterApi, SHPExecutionStage executionStage) {
    masterApi.getCounter(
        "SocialHashPartitioner Stats",
        String.format(counterName + " in %6d iteration after %d splits",
            executionStage.getIteration(), executionStage.getSplits())
      ).setValue(value);
  }

  public static void setCounter(
      String counterName, double value,
      BlockMasterApi masterApi, SHPExecutionStage executionStage) {
    masterApi.getCounter(
        "SocialHashPartitioner Stats",
        String.format(counterName + " in %6d iteration after %d splits (*K)",
            executionStage.getIteration(), executionStage.getSplits())
      ).setValue((long) (value * 1000));
  }
}
