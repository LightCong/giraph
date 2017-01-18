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
package org.apache.giraph.block_app.library.partitioning.goal.cache;

import org.apache.giraph.function.primitive.func.Int2DoubleFunction;

/** Int2DoubleFunction which caches [0, countToCache) values */
public class CachedGeneratedArray implements Int2DoubleFunction {
  private final int countToCache;
  // TODO make this not serialized
  private final double[] cached;
  private final Int2DoubleFunction function;

  public CachedGeneratedArray(
      int countToCache, Int2DoubleFunction function) {
    super();
    this.countToCache = countToCache;
    this.function = function;
    this.cached = generateCache();
  }

  private double[] generateCache() {
    double[] res = new double[countToCache];
    for (int i = 0; i < res.length; i++) {
      res[i] = function.apply(i);
    }
    return res;
  }

  @Override
  public double apply(int n) {
    return n < cached.length ? cached[n] : function.apply(n);
  }
}
