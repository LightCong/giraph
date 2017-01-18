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
package org.apache.giraph.block_app.library.partitioning.decide;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.apache.giraph.block_app.library.partitioning.SocialHashPartitionerSettings;
import org.apache.giraph.function.primitive.IntConsumer;
import org.apache.giraph.function.primitive.func.Byte2LongFunction;
import org.apache.giraph.function.primitive.func.Double2DoubleFunction;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/** Historgam Description Implementation */
public class HistogramDescImpl implements HistogramDesc {
  protected static final float NEG_SYM_OFFSET = 0.999f;

  private static final Logger LOG = Logger.getLogger(HistogramDescImpl.class);

  private final float[] binThresholds;

  public HistogramDescImpl(float[] binThresholds) {
    Preconditions.checkState(binThresholds.length < Byte.MAX_VALUE - 1);
    this.binThresholds = binThresholds;
  }

  public static HistogramDescImpl create(int quarterBins, float exponent) {
    int middle = quarterBins * 2;

    float[] binThresholds = new float[4 * quarterBins + 1];

    binThresholds[middle] = 0;
    for (int i = 1; i <= 2 * quarterBins; i++) {
      binThresholds[middle + i] = (float) Math.pow(exponent, i - quarterBins);
      binThresholds[middle - i] = - NEG_SYM_OFFSET * binThresholds[middle + i];
    }
    binThresholds[middle + 1] *= 1e-10;
    return new HistogramDescImpl(binThresholds);
  }

  @Override
  public byte toBin(float value) {
    int bin = Arrays.binarySearch(binThresholds, value);
    // Binary search returns -insertion point - 1 if the value is not found.
    if (bin < 0) {
      return (byte) (- bin - 1);
    } else {
      // boundaries are in the higher bucket
      return (byte) (bin + 1);
    }
  }

  @Override
  public float smallestValue(byte bin) {
    return bin > 0 ? binThresholds[bin - 1] : Float.NEGATIVE_INFINITY;
  }

  @Override
  public byte largestIndex() {
    return (byte) (binThresholds.length);
  }

  @Override
  public byte smallestIndex() {
    return 0;
  }

  @Override
  public void computeWhatToSwap(
      int[] bucket, long[] bucketSize, float allowedSize,
      float moveProbability, float maxMoveRatio,
      Byte2LongFunction[] getMoveCandidates,
      byte[] bins, float[] probs) {
    int left = 0;
    int right = 1;
    long roundedAllowedSize = Math.max(
        (long) allowedSize - 2,
        (bucketSize[left] + bucketSize[right] + 1) / 2);

    byte[] lastBin = new byte[2];
    float[] lastBinProb = new float[2];

    Byte2LongFunction[] newGetMoveCandidates =
        Arrays.copyOf(getMoveCandidates, 2);

    if (maxMoveRatio < 1) {
      long maxMove = (long) (roundedAllowedSize * maxMoveRatio);
      for (int side = 0; side < 2; side++) {
        long sum = 0;
        long[] moveCandidates = new long[largestIndex() + 1];
        for (byte bin = largestIndex(); bin >= smallestIndex(); bin--) {
          moveCandidates[bin] = getMoveCandidates[side].apply(bin);
          if (sum + moveCandidates[bin] >= maxMove) {
            lastBin[side] = bin;
            lastBinProb[side] =
                ((float) (maxMove - sum)) / moveCandidates[bin];
            Preconditions.checkState(
                !Float.isNaN(lastBinProb[side]) && lastBinProb[side] <= 1.0  &&
                lastBinProb[side] >= 0.0);
            moveCandidates[bin] = maxMove - sum;
            break;
          }
          sum += moveCandidates[bin];
        }
        Preconditions.checkState(
            LongStream.of(moveCandidates).sum() <= maxMove);
        newGetMoveCandidates[side] = (bin) -> moveCandidates[bin];
      }
    }

    computeWhatToSwapImpl(
        bucket, bucketSize, roundedAllowedSize, moveProbability,
        newGetMoveCandidates, bins, probs);

    if (maxMoveRatio < 1) {
      for (int side = 0; side < 2; side++) {
        if (bins[bucket[side]] < lastBin[side]) {
          Preconditions.checkState(bins[bucket[side]] >= lastBin[side]);
        }
        if (bins[bucket[side]] == lastBin[side]) {
          probs[bucket[side]] *= lastBinProb[side];
          Preconditions.checkState(!Float.isNaN(probs[bucket[side]]));
        }
      }
    }

    // Following are just consistency checks
    for (int side = 0; side < 2; side++) {
      Preconditions.checkState(bins[bucket[side]] > largestIndex() ||
          newGetMoveCandidates[side].apply(bins[bucket[side]]) > 0);
    }

    double[] movingTo = new double[2];
    for (int side = 0; side < 2; side++) {
      for (byte bin = bins[bucket[side]]; bin <= largestIndex(); bin++) {
        long candidates = getMoveCandidates[side].apply(bin);
        if (bin == bins[bucket[side]]) {
          movingTo[side] += candidates * probs[bucket[side]];
        } else {
          movingTo[side] += candidates;
        }
      }
    }
    double[] curSize = new double[2];
    curSize[left] = bucketSize[left] + movingTo[left] - movingTo[right];
    curSize[right] = bucketSize[right] + movingTo[right] - movingTo[left];

    double[] curSizeProb = new double[2];
    curSizeProb[left] =
        bucketSize[left] +
        (movingTo[left] - movingTo[right]) * moveProbability;
    curSizeProb[right] =
        bucketSize[right] +
        (movingTo[right] - movingTo[left]) * moveProbability;

    boolean shouldLog =
        SocialHashPartitionerSettings.IS_DEBUG || bucket[right] < 4;
    if (shouldLog) {
      LOG.info("Rounded allowed size " + roundedAllowedSize + ", buckets: " +
          Arrays.toString(bucket) + ", sizes: " + Arrays.toString(bucketSize) +
          ", expected: " + Arrays.toString(curSize) +
          ", expected with mov_prob: " + Arrays.toString(curSizeProb) +
          ", moving to: " + Arrays.toString(movingTo));
    }

    Preconditions.checkState(
        roundedAllowedSize + 0.01 >= curSize[left] ||
        roundedAllowedSize + 0.01 >= curSize[right]);

    double[] checkSize =
        (bucketSize[left] <= roundedAllowedSize &&
        bucketSize[right] <= roundedAllowedSize) ?
        curSize : curSizeProb;

    Double2DoubleFunction imprecision = (val) -> val * 1.00001 + 0.01;
    for (int side = 0; side < 2; side++) {
      if (imprecision.apply(roundedAllowedSize) < checkSize[side]) {
        if (checkSize[side] > imprecision.apply(bucketSize[side])) {
          Preconditions.checkState(
              checkSize[side] <= bucketSize[side] + 0.1,
              Arrays.toString(curSize) + "\n" +
              Arrays.toString(bucket) + "\n" +
              Arrays.toString(bucketSize)  + "\n" +
              allowedSize + " " + moveProbability + " " + maxMoveRatio + "\n" +
              IntStream.range(smallestIndex(), largestIndex() + 1)
                .mapToObj(i -> "" + getMoveCandidates[left].apply((byte) i))
                .collect(Collectors.joining(", ")) + "\n" +
              IntStream.range(smallestIndex(), largestIndex() + 1)
                .mapToObj(i -> "" + getMoveCandidates[right].apply((byte) i))
                .collect(Collectors.joining(", ")) + "\n" +
              Arrays.toString(bins) + "\n" + Arrays.toString(probs));
        }
      }
    }
  }

  private void computeWhatToSwapImpl(
      int[] bucket, long[] bucketSize, long roundedAllowedSize,
      float moveProbability, Byte2LongFunction[] getMoveCandidates,
      byte[] bins, float[] probs) {
    int left = 0;
    int right = 1;

    if (bucketSize[left] <= roundedAllowedSize &&
        bucketSize[right] <= roundedAllowedSize) {
      // if we are under limits, exclude probability, to keep expectation
      // within it, but use it if we are not - as balancing otherwise wouldn't
      // be enough.
      moveProbability = 1;
    }

    byte[] nextIndex = new byte[2];
    byte[] lastUsedIndex = new byte[2];
    long[] movingTo = new long[2];
    double[] curSize = new double[2];
    for (int side = 0; side < 2; side++) {
      nextIndex[side] = largestIndex();
      lastUsedIndex[side] = (byte) (largestIndex() + 1);
    }

    IntConsumer fillMoveAllPicked = side -> {
      bins[bucket[side]] = lastUsedIndex[side];
      Preconditions.checkState(bins[bucket[side]] > largestIndex() ||
          getMoveCandidates[side].apply(bins[bucket[side]]) > 0);
      probs[bucket[side]] = 1;
    };

    while (true) {
      int to;
      int from;
      curSize[left] = bucketSize[left] +
          (movingTo[left] - movingTo[right]) * moveProbability;
      curSize[right] = bucketSize[right] +
          (movingTo[right] - movingTo[left]) * moveProbability;

      Preconditions.checkState(
          roundedAllowedSize >= curSize[left] ||
          roundedAllowedSize >= curSize[right]);

      if (roundedAllowedSize < curSize[left] ||
          roundedAllowedSize < curSize[right]) {
        // if over imbalance, pick side to balance
        if (roundedAllowedSize < curSize[left]) {
          to = right;
          from = left;
        } else {
          to = left;
          from = right;
        }

        // if we moved too much, and gains are not positive any more, end
        // otherwise we will balance with negatives too
        if (lastUsedIndex[from] <= largestIndex() &&
            (nextIndex[to] < 0 ||
              (smallestValue((byte) (nextIndex[from] + 1)) +
                  smallestValue(nextIndex[to]) <= 0))) {
          fillMoveAllPicked.apply(to);
          fillMoveAllPicked.apply(from);

          float candidates =
              getMoveCandidates[from].apply(lastUsedIndex[from]) *
              moveProbability;
          Preconditions.checkState(candidates > 0);
          double toLeave = curSize[from] - roundedAllowedSize;
          if (toLeave >= candidates) {
            bins[bucket[from]]++;
            while (bins[bucket[from]] <= largestIndex() &&
                getMoveCandidates[from].apply(bins[bucket[from]]) == 0) {
              bins[bucket[from]]++;
            }
            Preconditions.checkState(
                bins[bucket[from]] > largestIndex() ||
                getMoveCandidates[from].apply(bins[bucket[from]]) > 0);
          } else {
            probs[bucket[from]] = (float) (1 - toLeave / candidates);
            Preconditions.checkState(
                (!Float.isNaN(probs[bucket[from]])) &&
                (probs[bucket[from]] >= 0));
          }
          return;
        }
      } else {
        // if balanced enough, pick higher gain
        if (nextIndex[left] == nextIndex[right]) {
          // if gains equal, pick first going to the smaller bucket:
          if (curSize[left] < curSize[right]) {
            to = left;
            from = right;
          } else {
            to = right;
            from = left;
          }
        } else if (nextIndex[left] >= nextIndex[right]) {
          to = left;
          from = right;
        } else {
          to = right;
          from = left;
        }

        Preconditions.checkState(nextIndex[to] > 0);
        float nextMinGain = smallestValue(nextIndex[to]);

        // If best gain is negative, end
        if (nextMinGain < 0) {
          fillMoveAllPicked.apply(from);
          fillMoveAllPicked.apply(to);
          return;
        }

        // if best gain is zero, only balance and end
        if (nextMinGain == 0) {
          fillMoveAllPicked.apply(from);
          long candidates = getMoveCandidates[to].apply(nextIndex[to]);
          if (curSize[to] + 1 < curSize[from] && candidates > 0) {
            bins[bucket[to]] = nextIndex[to];
            Preconditions.checkState(
                bins[bucket[to]] > largestIndex() ||
                getMoveCandidates[to].apply(bins[bucket[to]]) > 0);
            probs[bucket[to]] =
                ((float) Math.min(
                    candidates,
                    (curSize[from] - curSize[to]) / 2)) / candidates;
            Preconditions.checkState(!Float.isNaN(probs[bucket[to]]));
          } else {
            fillMoveAllPicked.apply(to);
          }
          return;
        }
      }

      if (nextIndex[to] < 0) {
        fillMoveAllPicked.apply(left);
        fillMoveAllPicked.apply(right);
        return;
      }
      long candidates = getMoveCandidates[to].apply(nextIndex[to]);
      if (candidates > 0) {
        lastUsedIndex[to] = nextIndex[to];
        movingTo[to] += candidates;
      }
      nextIndex[to]--;
    }
  }
}
