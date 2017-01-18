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
package org.apache.giraph.block_app.library.partitioning.recursive;

/**
 * Abstracts any movement restrictions (currently supports only
 * consecutive chunks restrictions).
 */
public interface MoveBucketLimiter {
  /**
   * Returns the first bucket allowable for a vertex
   * in the currentBucket.
   *
   * @param currentBucket current bucket
   * @return first allowable bucket
   */
  int startBucket(int currentBucket);

  /**
   * Returns the last bucket allowable + 1 for a vertex
   * in the currentBucket.
   *
   * @param currentBucket current bucket
   * @return last allowable bucket + 1
   */
  int endBucket(int currentBucket);

  int otherBucket(int currentBucket);

  default boolean inside(int currentBucket, int checkBucket) {
    return startBucket(currentBucket) <= checkBucket &&
        checkBucket < endBucket(currentBucket);
  }
}
