package org.apache.lucene.analysis;

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

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.BigNumericUtils;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.math.BigInteger;

public class TestBigNumericTokenStream extends BaseTokenStreamTestCase {

  static final long lvalue = 4573245871874382L;

  @Test
  public void testLongStream() throws Exception {
    final BigNumericTokenStream stream=new BigNumericTokenStream(4, 64).setBigIntValue(BigInteger.valueOf(lvalue));
    // use getAttribute to test if attributes really exist, if not an IAE will be throwed
    final TermToBytesRefAttribute bytesAtt = stream.getAttribute(TermToBytesRefAttribute.class);
    final TypeAttribute typeAtt = stream.getAttribute(TypeAttribute.class);
    final BigNumericTokenStream.NumericTermAttribute numericAtt = stream.getAttribute(BigNumericTokenStream.NumericTermAttribute.class);
    final BytesRef bytes = bytesAtt.getBytesRef();
    stream.reset();
    assertEquals(64, numericAtt.getValueSize());
    for (int shift=0; shift<64; shift+=BigNumericUtils.PRECISION_STEP_DEFAULT) {
      assertTrue("New token is available", stream.incrementToken());
      assertEquals("Shift value wrong", shift, numericAtt.getShift());
      final int hash = bytesAtt.fillBytesRef();
      assertEquals("Hash incorrect", bytes.hashCode(), hash);
      assertEquals("Term is incorrectly encoded", BigInteger.valueOf(lvalue & ~((1L << shift) - 1L)), BigNumericUtils.prefixCodedToBigInteger(bytes));
      assertEquals("Term raw value is incorrectly encoded", BigInteger.valueOf(lvalue & ~((1L << shift) - 1L)), numericAtt.getRawValue());
      assertEquals("Type incorrect", (shift == 0) ? BigNumericTokenStream.TOKEN_TYPE_FULL_PREC : BigNumericTokenStream.TOKEN_TYPE_LOWER_PREC, typeAtt.type());
    }
    assertFalse("More tokens available", stream.incrementToken());
    stream.end();
    stream.close();
  }


  @Test
  public void testNotInitialized() throws Exception {
    final BigNumericTokenStream stream=new BigNumericTokenStream();
    
    try {
      stream.reset();
      fail("reset() should not succeed.");
    } catch (IllegalStateException e) {
      // pass
    }

    try {
      stream.incrementToken();
      fail("incrementToken() should not succeed.");
    } catch (IllegalStateException e) {
      // pass
    }
  }
  
  public static interface TestAttribute extends CharTermAttribute {}
  public static class TestAttributeImpl extends CharTermAttributeImpl implements TestAttribute {}

  @Test
  public void testCTA() throws Exception {
    final BigNumericTokenStream stream=new BigNumericTokenStream();
    try {
      stream.addAttribute(CharTermAttribute.class);
      fail("Succeeded to add CharTermAttribute.");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().startsWith("BigNumericTokenStream does not support"));
    }
    try {
      stream.addAttribute(TestAttribute.class);
      fail("Succeeded to add TestAttribute.");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().startsWith("BigNumericTokenStream does not support"));
    }
  }
  
}
