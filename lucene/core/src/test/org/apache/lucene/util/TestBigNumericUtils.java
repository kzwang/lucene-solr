package org.apache.lucene.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.junit.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;


public class TestBigNumericUtils extends LuceneTestCase {


  @Test
  public void testBigIntegerConversionAndOrdering() throws Exception {
    // generate a series of encoded longs, each numerical one bigger than the one before
    BytesRef last=null, act=new BytesRef(BigNumericUtils.getBufferSize(128));
    for (long l=0; l<100000L; l++) {
      BigNumericUtils.bigIntegerToPrefixCoded(BigInteger.valueOf(l), 0, act, 128);
      if (last!=null) {
        // test if smaller
        assertTrue("actual bigger than last (BytesRef)", last.compareTo(act) < 0 );
        assertTrue("actual bigger than last (as String)", last.utf8ToString().compareTo(act.utf8ToString()) < 0 );
      }
      // test is back and forward conversion works
      assertEquals("forward and back conversion should generate same long", BigInteger.valueOf(l), BigNumericUtils.prefixCodedToBigInteger(act));
      // next step
      last = act;
      act = new BytesRef(BigNumericUtils.getBufferSize(128));
    }
  }

  private void assertBigIntegerRangeSplit(final BigInteger lower, final BigInteger upper, int precisionStep,
                                    final Iterable<BigInteger> expectedBounds, final Iterable<Integer> expectedShifts,
                                    final int valueSize
  ) {
    final Iterator<BigInteger> neededBounds = (expectedBounds == null) ? null : expectedBounds.iterator();
    final Iterator<Integer> neededShifts = (expectedShifts == null) ? null : expectedShifts.iterator();

    BigNumericUtils.splitRange(new BigNumericUtils.BigIntegerRangeBuilder() {
      @Override
      public void addRange(BigInteger min, BigInteger max, int shift, final int valueSize) {
        assertTrue("min, max should be inside bounds", min.compareTo(lower) >= 0 && min.compareTo(upper) <= 0
            && max.compareTo(lower) >= 0 && max.compareTo(upper) <= 0);
        if (neededBounds == null || neededShifts == null)
          return;
        assertEquals("shift", neededShifts.next().intValue(), shift);
        assertEquals("inner min bound", neededBounds.next(), BigNumericUtils.parseBigInteger(min, valueSize).shiftRight(shift));
        assertEquals("inner max bound", neededBounds.next(), BigNumericUtils.parseBigInteger(max, valueSize).shiftRight(shift));
      }
    }, valueSize, precisionStep, lower, upper);

  }


  @Test
  public void testRandomSplit() throws Exception {
    long num = (long) atLeast(10);
    for (long i=0; i < num; i++) {
      executeOneRandomSplit(random());
    }
  }

  private void executeOneRandomSplit(final Random random) throws Exception {
    long lower = randomLong(random);
    long len = random.nextInt(16384*1024); // not too large bitsets, else OOME!
    while (lower + len < lower) { // overflow
      lower >>= 1;
    }
    assertBigIntegerRangeSplit(BigInteger.valueOf(lower), BigInteger.valueOf(lower + len), random.nextInt(64) + 1, null, null, 128);
  }

  @Test
  public void testSplitBigIntegerRange() throws Exception {
    // a hard-coded "standard" range
    assertBigIntegerRangeSplit(BigInteger.valueOf(-5000L), BigInteger.valueOf(9500L), 4, Arrays.asList(
        BigInteger.valueOf(0x7fffffffffffec78L), BigInteger.valueOf(0x7fffffffffffec7fL),
        BigInteger.valueOf(0x2510L), BigInteger.valueOf(0x251cL),
        BigInteger.valueOf(0x7fffffffffffec8L), BigInteger.valueOf(0x7fffffffffffecfL),
        BigInteger.valueOf(0x250L), BigInteger.valueOf(0x250L),
        BigInteger.valueOf(0x7fffffffffffedL), BigInteger.valueOf(0x7fffffffffffefL),
        BigInteger.valueOf(0x20L), BigInteger.valueOf(0x24L),
        BigInteger.valueOf(0x7ffffffffffffL), BigInteger.valueOf(0x1L)
    ), Arrays.asList(
        0, 0,
        4, 4,
        8, 8,
        12
    ), 64);

    assertBigIntegerRangeSplit(BigInteger.valueOf(-5000L), BigInteger.valueOf(9500L), 64, Arrays.asList(
        BigInteger.valueOf(0x7fffffffffffec78L),BigInteger.valueOf(0x251cL)
    ), Arrays.asList(
        0
    ), 64);

    assertBigIntegerRangeSplit(BigInteger.ZERO, BigInteger.valueOf(1024L+63L), 4, Arrays.asList(
        BigInteger.valueOf(0x40L), BigInteger.valueOf(0x43L),
        BigInteger.valueOf(0x0L),  BigInteger.valueOf(0x3L)
    ), Arrays.asList(
        4, 8
    ), 64);
  }

  private long randomLong(final Random random) {
    long val;
    switch(random.nextInt(4)) {
      case 0:
        val = 1L << (random.nextInt(63)); //  patterns like 0x000000100000 (-1 yields patterns like 0x0000fff)
        break;
      case 1:
        val = -1L << (random.nextInt(63)); // patterns like 0xfffff00000
        break;
      default:
        val = random.nextLong();
    }

    val += random.nextInt(5)-2;

    if (random.nextBoolean()) {
      if (random.nextBoolean()) val += random.nextInt(100)-50;
      if (random.nextBoolean()) val = ~val;
      if (random.nextBoolean()) val = val<<1;
      if (random.nextBoolean()) val = val>>>1;
    }

    return val;
  }


}
