package org.apache.giraph.block_app.library.partitioning.decide;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.giraph.function.primitive.func.Byte2LongFunction;
import org.junit.Test;

import it.unimi.dsi.fastutil.bytes.Byte2LongOpenHashMap;

public class HistogramDescTest {
  private static final float EPS = 1e-5f;

  @Test
  public void testBins() {
    HistogramDesc hist = HistogramDescImpl.create(2, 2);

    assertEquals(9, hist.largestIndex());

    assertEquals(Float.NEGATIVE_INFINITY, hist.smallestValue((byte) 0), EPS);

    assertEquals(- 4 * HistogramDescImpl.NEG_SYM_OFFSET, hist.smallestValue((byte) 1), EPS);
    assertEquals(- 2 * HistogramDescImpl.NEG_SYM_OFFSET, hist.smallestValue((byte) 2), EPS);
    assertEquals(- 1 * HistogramDescImpl.NEG_SYM_OFFSET, hist.smallestValue((byte) 3), EPS);
    assertEquals(- 0.5 * HistogramDescImpl.NEG_SYM_OFFSET, hist.smallestValue((byte) 4), EPS);

    assertEquals(0, hist.smallestValue((byte) 5), EPS);
    assertEquals(0.5, hist.smallestValue((byte) 6), EPS);
    assertEquals(1, hist.smallestValue((byte) 7), EPS);
    assertEquals(2, hist.smallestValue((byte) 8), EPS);
    assertEquals(4, hist.smallestValue((byte) 9), EPS);

    assertEquals(8, hist.toBin(2));
    assertEquals(8, hist.toBin(2 + EPS));
    assertEquals(7, hist.toBin(2 - EPS));
  }

  private static void concreteExample(
      long[] bucketSizes,
      float allowedSize, float moveProbability, float maxMoveRatio,
      long[] leftMoveCandidates,
      long[] rightMoveCandidates,
      byte[] expectedBins, float[] expectedProbs
      ) {
    HistogramDesc hist = HistogramDescImpl.create(30, (float) Math.pow(2, 0.5));
    byte[] bins = new byte[2];
    float[] probs = new float[2];
    hist.computeWhatToSwap(
        new int[] {0, 1},
        bucketSizes,
        allowedSize, moveProbability, maxMoveRatio,
        new Byte2LongFunction[] {
          bin -> leftMoveCandidates[bin],
          bin -> rightMoveCandidates[bin]
        }, bins, probs);
    assertEquals(expectedBins[0], bins[0]);
    assertEquals(expectedProbs[0], probs[0], EPS);
    assertEquals(expectedBins[1], bins[1]);
    assertEquals(expectedProbs[1], probs[1], EPS);
  }

  @Test
  public void testConcreteExample1() {
    concreteExample(
        new long[] {3077496, 3076168},
        3093583.5f, 0.8f, 0.1f,
        new long[] {
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 1382, 21619, 72606, 112344, 121484, 183594, 156313, 121986, 84027, 92026, 58414, 77552, 46004, 65163, 39110, 50724, 33211, 42052, 28679, 34457, 25232, 8673, 8078, 9053, 8836, 9411, 9788, 9350, 10062, 10527, 10663, 10367, 10738, 108919, 1093194, 18028, 3374, 16775, 3767, 16590, 3890, 15177, 3637, 13471, 3555, 12864, 3531, 30526, 8418, 22612, 7218, 13443, 5830, 11665, 6672, 11480, 6879, 11394, 7389, 11736, 7258, 6853, 3420, 1915, 812, 302, 45, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        },
        new long[] {
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 14, 1410, 22257, 74387, 112939, 122726, 183878, 158154, 121899, 84570, 91542, 57802, 77768, 45971, 64265, 38725, 50560, 32780, 42149, 28476, 33549, 25183, 8307, 7850, 8553, 8676, 8883, 9238, 10051, 9837, 9942, 10365, 9629, 10713, 110208, 1092378, 17593, 3370, 17110, 3544, 16782, 3975, 15307, 3750, 14539, 3898, 12932, 3675, 30541, 8395, 22540, 7299, 13353, 5814, 11997, 6504, 11290, 6952, 11341, 7331, 11470, 7142, 6750, 3603, 1847, 845, 335, 38, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        },
        new byte[] { 61, 62 },
        new float[] { 0.000614f, 1.0f});
  }

  @Test
  public void testConcreteExample2() {
    concreteExample(
        new long[] {1515643, 1555203},
        1555167.8f, 0.8f, 0.1f,
        new long[] {
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 575, 14057, 86413, 170193, 173141, 157592, 143723, 221652, 107161, 127341, 62052, 93122, 41273, 62860, 24811, 29574, 12393, 8087, 4726, 4103, 2021, 2330, 1078, 333, 183, 224, 146, 192, 133, 176, 118, 160, 101, 111, 71, 629, 895, 45, 15, 72, 22, 62, 28, 77, 29, 75, 54, 92, 34, 166, 64, 123, 48, 61, 32, 41, 41, 48, 39, 55, 34, 46, 20, 16, 5, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        },
        new long[] {
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 531, 9774, 41275, 71131, 71482, 76691, 70767, 48646, 49813, 32889, 36165, 24834, 28477, 20450, 24349, 17358, 22546, 16228, 22384, 15079, 23010, 5619, 7641, 4043, 6301, 3776, 5801, 3589, 5500, 3345, 4942, 3060, 4270, 26941, 195386, 10638, 5677, 11131, 6749, 11584, 7793, 11997, 8648, 12225, 9320, 12922, 10979, 40207, 34573, 41776, 35098, 37121, 28174, 24674, 21574, 17339, 17008, 16085, 15907, 16447, 15247, 13487, 9245, 5256, 2097, 498, 69, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        },
        new byte[] { 38, 84 },
        new float[] { 0.79067767f, 1.0f});
  }

  @Test
  public void testConcreteExample3() {
    concreteExample(
        new long[] {773227, 781808},
        781771.75f, 0.8f, 0.1f,
        new long[] {
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6067, 61301, 98082, 173628, 222388, 163516, 48898, 6538, 987, 281, 79, 22, 5, 1, 1, 2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 1, 2, 0, 0, 1, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        },
        new long[] {
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 413, 5474, 19873, 30222, 30322, 26050, 19962, 15016, 10950, 7885, 5663, 3973, 2790, 2067, 1429, 970, 744, 529, 376, 232, 184, 120, 96, 76, 53, 33, 32, 55, 53, 24, 34, 35, 62, 93, 130, 193, 236, 338, 478, 720, 1032, 1519, 2071, 2952, 4157, 5977, 8541, 12557, 17651, 25957, 38027, 60704, 112164, 134663, 94372, 48894, 13011, 853, 118, 17, 4, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        },
        new byte[] { 33, 89 },
        new float[] { 0.34266233f, 1.0f});
  }

  @Test
  public void testConcreteExample4() {
    concreteExample(
        new long[] {300, 298},
        299.55576f, 0.8f, 0.1f,
        new long[] {
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 4, 6, 5, 7, 9, 9, 20, 15, 16, 13, 7, 12, 7, 10, 4, 2, 0, 3, 0, 1, 1, 2, 2, 1, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 2, 0, 2, 0, 2, 0, 0, 1, 0, 5, 3, 3, 9, 9, 8, 13, 7, 11, 17, 13, 8, 8, 8, 2, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        },
        new long[] {
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 6, 7, 7, 13, 11, 13, 15, 5, 11, 12, 10, 9, 7, 2, 2, 2, 3, 0, 3, 1, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 4, 2, 2, 0, 6, 2, 7, 5, 15, 5, 15, 14, 10, 20, 17, 11, 6, 2, 2, 2, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        },
        new byte[] { 90, 90 },
        new float[] { 0.9687481f, 0.23529f});
  }

  @Test
  public void testConcreteExample5() {
    concreteExample(
        new long[] {633331, 608902},
        633213.94f, 0.8f, 0.1f,
        new long[] {
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 47, 258, 629, 1047, 2249, 3803, 6124, 9635, 14085, 19718, 25600, 30271, 34577, 36340, 35570, 33428, 27054, 20720, 16493, 12041, 9715, 7181, 5821, 4736, 3711, 3203, 2511, 2005, 1660, 1591, 1234, 1108, 879, 281, 227, 182, 164, 200, 150, 199, 83, 119, 74, 1165, 13084, 263, 43, 340, 55, 388, 81, 464, 102, 615, 175, 2921, 763, 3484, 1088, 4363, 1960, 5596, 3652, 7253, 5960, 10366, 9798, 14698, 16360, 22849, 27448, 34631, 23144, 10829, 4669, 2014, 913, 377, 165, 56, 30, 6, 7, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        },
        new long[] {
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 5, 5, 20, 33, 81, 163, 297, 564, 1178, 2538, 5564, 12209, 22936, 37225, 52651, 67670, 78517, 83639, 85410, 76760, 56153, 38316, 4776, 1983, 954, 509, 270, 177, 114, 86, 59, 41, 21, 12, 15, 11, 7, 7, 3, 2, 2, 0, 1, 0, 0, 0, 0, 1, 0, 6, 22, 1, 0, 0, 1, 2, 1, 2, 2, 4, 1, 6, 3, 4, 6, 8, 9, 25, 28, 39, 45, 69, 97, 148, 147, 181, 182, 226, 262, 217, 175, 159, 109, 61, 49, 23, 10, 9, 1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        },
        new byte[] { 90, 32 },
        new float[] { 1.0f, 0.99327344f});
  }

  @Test
  public void testConcreteExample6() {
    concreteExample(
        new long[] {3092325, 3061339},
        3092216.2f, 0.8f, 0.1f,
        new long[] {
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 870, 33355, 228470, 433618, 438235, 370844, 335231, 320689, 368419, 232206, 142955, 77220, 36397, 16593, 8984, 5244, 3378, 1979, 1256, 937, 647, 372, 326, 235, 110, 68, 74, 67, 60, 50, 55, 40, 29, 268, 217, 67, 14, 12, 8, 10, 21, 26, 20, 42, 56, 88, 83, 103, 79, 105, 105, 132, 110, 122, 176, 144, 95, 129, 45, 28, 14, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        },
        new long[] {
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 27, 2165, 27339, 94172, 147656, 153030, 138046, 122273, 108914, 88541, 83890, 69922, 60885, 52460, 49996, 41073, 36114, 31953, 33362, 26191, 22143, 18651, 10753, 7796, 6526, 5341, 4478, 3732, 3219, 3184, 2819, 26417, 116437, 60511, 7417, 6621, 6954, 7568, 10491, 9994, 11677, 14454, 34254, 41373, 49789, 70529, 79202, 83515, 95138, 114756, 114194, 119667, 120509, 122153, 98144, 78533, 60643, 39664, 20624, 9818, 3648, 879, 93, 7, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        },
        new byte[] { 38, 84 },
        new float[] { 1.0f, 0.7596791f});
  }

  void testComputeWhatToSwap(
      long[] bucketSize, float allowedSize, float moveProbability, float maxMoveRatio,
      Byte2LongFunction[] getMoveCandidates,
      int expectedLeftBin, float expectedLeftProb,
      int expectedRightBin, float expectedRightProb) {
    HistogramDesc hist = HistogramDescImpl.create(2, 2);
    byte[] bins = new byte[2];
    float[] probs = new float[2];
    hist.computeWhatToSwap(
        new int[] {0, 1}, bucketSize, allowedSize, moveProbability, maxMoveRatio, getMoveCandidates, bins, probs);

    System.out.println(Arrays.toString(bins) + " " + Arrays.toString(probs));
    assertEquals(expectedLeftBin, bins[0]);
    assertEquals(expectedLeftProb, probs[0], EPS);
    assertEquals(expectedRightBin, bins[1]);
    assertEquals(expectedRightProb, probs[1], EPS);
  }

  Byte2LongFunction toF(int... values) {
    Byte2LongOpenHashMap map = new Byte2LongOpenHashMap();
    for (int i = 0; i < values.length; i+= 2) {
      map.put((byte) values[i], values[i + 1]);
    }
    return map::get;
  }

  @Test
  public void testMovement() {
    testComputeWhatToSwap(
        new long[] {10, 10}, 10, 1.0f, 1.0f, new Byte2LongFunction[] {
          toF(9, 2),
          toF(5, 1),
        },
        9, 0.5f, 5, 1f);
    testComputeWhatToSwap(
        new long[] {10, 10}, 10, 1.0f, 1.0f, new Byte2LongFunction[] {
          toF(9, 3),
          toF(5, 1),
        },
        9, 1.0f/3, 5, 1f);

    testComputeWhatToSwap(
        new long[] {10, 0}, 5, 1.0f, 1.0f, new Byte2LongFunction[] {
          toF(),
          toF(0, 10),
        },
        10, 1f, 0, 0.5f);

    testComputeWhatToSwap(
        new long[] {15, 5}, 10, 1.0f, 0.5f, new Byte2LongFunction[] {
          toF(),
          toF(0, 10),
        },
        10, 1f, 0, 0.5f);
    testComputeWhatToSwap(
        new long[] {20, 10}, 15, 1.0f, 0.5f, new Byte2LongFunction[] {
          toF(),
          toF(0, 10),
        },
        10, 1f, 0, 0.5f);

    testComputeWhatToSwap(
        new long[] {10, 10}, 10, 1.0f, 1.0f, new Byte2LongFunction[] {
          toF(3, 2, 4, 2, 5, 2, 6, 2, 7, 2),
          toF(3, 2, 4, 2, 5, 2, 6, 2, 7, 2),
        },
        6, 1f, 6, 1f);

    testComputeWhatToSwap(
        new long[] {16, 10}, 13, 1.0f, 1.0f, new Byte2LongFunction[] {
          toF(3, 2, 4, 2, 5, 2, 6, 2, 7, 2),
          toF(3, 2, 4, 2, 5, 2, 6, 2, 7, 2),
        },
        6, 1f, 4, 0.5f);
  }

  @Test
  public void testMovementWithAllowedImbalance() {
    testComputeWhatToSwap(
        new long[] {10, 10}, 11, 1.0f, 1.0f, new Byte2LongFunction[] {
          toF(9, 2),
          toF(5, 1),
        },
        9, 1f, 5, 1f);
    testComputeWhatToSwap(
        new long[] {10, 10}, 12, 1.0f, 1.0f, new Byte2LongFunction[] {
          toF(9, 2),
          toF(5, 1),
        },
        9, 1f, 10, 1f);

    testComputeWhatToSwap(
        new long[] {16, 10}, 16, 1.0f, 1.0f, new Byte2LongFunction[] {
          toF(3, 2, 4, 2, 5, 2, 6, 2, 7, 2),
          toF(3, 2, 4, 2, 5, 2, 6, 2, 7, 2),
        },
        6, 1f, 6, 1f);
    testComputeWhatToSwap(
        new long[] {16, 10}, 17, 1.0f, 1.0f, new Byte2LongFunction[] {
          toF(3, 2, 4, 2, 5, 2, 6, 2, 7, 2),
          toF(3, 2, 4, 2, 5, 2, 6, 2, 7, 2),
        },
        6, 1f, 6, 1f);
    testComputeWhatToSwap(
        new long[] {16, 10}, 14, 1.0f, 1.0f, new Byte2LongFunction[] {
          toF(3, 2, 4, 2, 5, 2, 6, 2, 7, 2),
          toF(3, 2, 4, 2, 5, 2, 6, 2, 7, 2),
        },
        6, 1f, 5, 1f);
  }
}
