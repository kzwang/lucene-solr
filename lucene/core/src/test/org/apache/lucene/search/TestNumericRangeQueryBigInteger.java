package org.apache.lucene.search;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BigIntegerField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BigNumericUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigInteger;

public class TestNumericRangeQueryBigInteger extends LuceneTestCase {
  // distance of entries
  private static long distance;
  // shift the starting of the values to the left, to also have negative values:
  private static final long startOffset = - 1L << 31;
  // number of docs to generate for testing
  private static int noDocs;
  
  private static Directory directory = null;
  private static IndexReader reader = null;
  private static IndexSearcher searcher = null;

  private static int valueSize = 64;
  @BeforeClass
  public static void beforeClass() throws Exception {
    noDocs = atLeast(4096);
    distance = (1L << 60) / noDocs;
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000))
        .setMergePolicy(newLogMergePolicy()));

    final FieldType storedLong = new FieldType(BigIntegerField.TYPE_NOT_STORED);
    storedLong.setStored(true);
    storedLong.freeze();

    final FieldType storedLong8 = new FieldType(storedLong);
    storedLong8.setNumericPrecisionStep(8);

    final FieldType storedLong4 = new FieldType(storedLong);
    storedLong4.setNumericPrecisionStep(4);

    final FieldType storedLong6 = new FieldType(storedLong);
    storedLong6.setNumericPrecisionStep(6);

    final FieldType storedLong2 = new FieldType(storedLong);
    storedLong2.setNumericPrecisionStep(2);

    final FieldType storedLongNone = new FieldType(storedLong);
    storedLongNone.setNumericPrecisionStep(Integer.MAX_VALUE);

    final FieldType unstoredLong = BigIntegerField.TYPE_NOT_STORED;

    final FieldType unstoredLong8 = new FieldType(unstoredLong);
    unstoredLong8.setNumericPrecisionStep(8);

    final FieldType unstoredLong6 = new FieldType(unstoredLong);
    unstoredLong6.setNumericPrecisionStep(6);

    final FieldType unstoredLong4 = new FieldType(unstoredLong);
    unstoredLong4.setNumericPrecisionStep(4);

    final FieldType unstoredLong2 = new FieldType(unstoredLong);
    unstoredLong2.setNumericPrecisionStep(2);

    BigIntegerField
      field8 = new BigIntegerField("field8", BigInteger.valueOf(0L), storedLong8, valueSize),
      field6 = new BigIntegerField("field6", BigInteger.valueOf(0L), storedLong6, valueSize),
      field4 = new BigIntegerField("field4", BigInteger.valueOf(0L), storedLong4, valueSize),
      field2 = new BigIntegerField("field2", BigInteger.valueOf(0L), storedLong2, valueSize),
      fieldNoTrie = new BigIntegerField("field"+Integer.MAX_VALUE, BigInteger.valueOf(0L), storedLongNone, valueSize),
      ascfield8 = new BigIntegerField("ascfield8", BigInteger.valueOf(0L), unstoredLong8, valueSize),
      ascfield6 = new BigIntegerField("ascfield6", BigInteger.valueOf(0L), unstoredLong6, valueSize),
      ascfield4 = new BigIntegerField("ascfield4", BigInteger.valueOf(0L), unstoredLong4, valueSize),
      ascfield2 = new BigIntegerField("ascfield2", BigInteger.valueOf(0L), unstoredLong2, valueSize);

    Document doc = new Document();
    // add fields, that have a distance to test general functionality
    doc.add(field8); doc.add(field6); doc.add(field4); doc.add(field2); doc.add(fieldNoTrie);
    // add ascending fields with a distance of 1, beginning at -noDocs/2 to test the correct splitting of range and inclusive/exclusive
    doc.add(ascfield8); doc.add(ascfield6); doc.add(ascfield4); doc.add(ascfield2);
    
    // Add a series of noDocs docs with increasing long values, by updating the fields
    for (int l=0; l<noDocs; l++) {
      long val=distance*l+startOffset;
      field8.setBigIntegerValue(BigInteger.valueOf(val));
      field6.setBigIntegerValue(BigInteger.valueOf(val));
      field4.setBigIntegerValue(BigInteger.valueOf(val));
      field2.setBigIntegerValue(BigInteger.valueOf(val));
      fieldNoTrie.setBigIntegerValue(BigInteger.valueOf(val));

      val=l-(noDocs/2);
      ascfield8.setBigIntegerValue(BigInteger.valueOf(val));
      ascfield6.setBigIntegerValue(BigInteger.valueOf(val));
      ascfield4.setBigIntegerValue(BigInteger.valueOf(val));
      ascfield2.setBigIntegerValue(BigInteger.valueOf(val));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    searcher=newSearcher(reader);
    writer.shutdown();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    searcher = null;
    reader.close();
    reader = null;
    directory.close();
    directory = null;
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // set the theoretical maximum term count for 8bit (see docs for the number)
    // super.tearDown will restore the default
    BooleanQuery.setMaxClauseCount(7*255*2 + 255);
  }
  
  /** test for constant score + boolean query + filter, the other tests only use the constant score mode */
  private void testRange(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    int count=3000;
    long lower=(distance*3/2)+startOffset, upper=lower + count*distance + (distance/3);
    NumericRangeQuery<BigInteger> q = NumericRangeQuery.newBigIntegerRange(field, precisionStep, BigInteger.valueOf(lower), BigInteger.valueOf(upper), true, true, valueSize);
    NumericRangeFilter<BigInteger> f = NumericRangeFilter.newBigIntegerRange(field, precisionStep, BigInteger.valueOf(lower), BigInteger.valueOf(upper), true, true, valueSize);
    for (byte i=0; i<3; i++) {
      TopDocs topDocs;
      String type;
      switch (i) {
        case 0:
          type = " (constant score filter rewrite)";
          q.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE);
          topDocs = searcher.search(q, null, noDocs, Sort.INDEXORDER);
          break;
        case 1:
          type = " (constant score boolean rewrite)";
          q.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
          topDocs = searcher.search(q, null, noDocs, Sort.INDEXORDER);
          break;
        case 2:
          type = " (filter)";
          topDocs = searcher.search(new MatchAllDocsQuery(), f, noDocs, Sort.INDEXORDER);
          break;
        default:
          return;
      }
      ScoreDoc[] sd = topDocs.scoreDocs;
      assertNotNull(sd);
      assertEquals("Score doc count"+type, count, sd.length );
      StoredDocument doc=searcher.doc(sd[0].doc);
      assertEquals("First doc"+type, 2*distance+startOffset, new BigInteger(doc.getField(field).stringValue()).longValue());
      doc=searcher.doc(sd[sd.length-1].doc);
      assertEquals("Last doc"+type, (1+count)*distance+startOffset, new BigInteger(doc.getField(field).stringValue()).longValue());
    }
  }

  @Test
  public void testRange_8bit() throws Exception {
    testRange(8);
  }
  
  @Test
  public void testRange_6bit() throws Exception {
    testRange(6);
  }
  
  @Test
  public void testRange_4bit() throws Exception {
    testRange(4);
  }
  
  @Test
  public void testRange_2bit() throws Exception {
    testRange(2);
  }
  
  @Test
  public void testInverseRange() throws Exception {
    AtomicReaderContext context = SlowCompositeReaderWrapper.wrap(searcher.getIndexReader()).getContext();
    NumericRangeFilter<BigInteger> f = NumericRangeFilter.newBigIntegerRange("field8", 8, BigInteger.valueOf(1000L), BigInteger.valueOf(-1000L), true, true, valueSize);
    assertNull("A inverse range should return the null instance", 
        f.getDocIdSet(context, context.reader().getLiveDocs()));
    f = NumericRangeFilter.newBigIntegerRange("field8", 8, BigInteger.valueOf(Long.MAX_VALUE), null, false, false, valueSize);
    assertNull("A exclusive range starting with Long.MAX_VALUE should return the null instance",
               f.getDocIdSet(context, context.reader().getLiveDocs()));
    f = NumericRangeFilter.newBigIntegerRange("field8", 8, null, BigInteger.valueOf(Long.MIN_VALUE), false, false, valueSize);
    assertNull("A exclusive range ending with Long.MIN_VALUE should return the null instance",
               f.getDocIdSet(context, context.reader().getLiveDocs()));
  }
  
  @Test
  public void testOneMatchQuery() throws Exception {
    NumericRangeQuery<BigInteger> q = NumericRangeQuery.newBigIntegerRange("ascfield8", 8, BigInteger.valueOf(1000L), BigInteger.valueOf(1000L), true, true, valueSize);
    TopDocs topDocs = searcher.search(q, noDocs);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", 1, sd.length );
  }
  
  private void testLeftOpenRange(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    int count=3000;
    long upper=(count-1)*distance + (distance/3) + startOffset;
    NumericRangeQuery<BigInteger> q = NumericRangeQuery.newBigIntegerRange(field, precisionStep, null, BigInteger.valueOf(upper), true, true, valueSize);
    TopDocs topDocs = searcher.search(q, null, noDocs, Sort.INDEXORDER);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", count, sd.length );
    StoredDocument doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", startOffset, new BigInteger(doc.getField(field).stringValue()).longValue());
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (count-1)*distance+startOffset, new BigInteger(doc.getField(field).stringValue()).longValue());

    q=NumericRangeQuery.newBigIntegerRange(field, precisionStep, null, BigInteger.valueOf(upper), false, true, valueSize);
    topDocs = searcher.search(q, null, noDocs, Sort.INDEXORDER);
    sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", count, sd.length );
    doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", startOffset, new BigInteger(doc.getField(field).stringValue()).longValue());
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (count-1)*distance+startOffset, new BigInteger(doc.getField(field).stringValue()).longValue());
  }
  
  @Test
  public void testLeftOpenRange_8bit() throws Exception {
    testLeftOpenRange(8);
  }
  
  @Test
  public void testLeftOpenRange_6bit() throws Exception {
    testLeftOpenRange(6);
  }
  
  @Test
  public void testLeftOpenRange_4bit() throws Exception {
    testLeftOpenRange(4);
  }
  
  @Test
  public void testLeftOpenRange_2bit() throws Exception {
    testLeftOpenRange(2);
  }
  
  private void testRightOpenRange(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    int count=3000;
    long lower=(count-1)*distance + (distance/3) +startOffset;
    NumericRangeQuery<BigInteger> q=NumericRangeQuery.newBigIntegerRange(field, precisionStep, BigInteger.valueOf(lower), null, true, true, valueSize);
    TopDocs topDocs = searcher.search(q, null, noDocs, Sort.INDEXORDER);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", noDocs-count, sd.length );
    StoredDocument doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", count*distance+startOffset, new BigInteger(doc.getField(field).stringValue()).longValue());
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (noDocs-1)*distance+startOffset, new BigInteger(doc.getField(field).stringValue()).longValue());

    q=NumericRangeQuery.newBigIntegerRange(field, precisionStep, BigInteger.valueOf(lower), null, true, false, valueSize);
    topDocs = searcher.search(q, null, noDocs, Sort.INDEXORDER);
    sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", noDocs-count, sd.length );
    doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", count*distance+startOffset, new BigInteger(doc.getField(field).stringValue()).longValue());
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (noDocs-1)*distance+startOffset, new BigInteger(doc.getField(field).stringValue()).longValue());
  }
  
  @Test
  public void testRightOpenRange_8bit() throws Exception {
    testRightOpenRange(8);
  }
  
  @Test
  public void testRightOpenRange_6bit() throws Exception {
    testRightOpenRange(6);
  }
  
  @Test
  public void testRightOpenRange_4bit() throws Exception {
    testRightOpenRange(4);
  }
  
  @Test
  public void testRightOpenRange_2bit() throws Exception {
    testRightOpenRange(2);
  }

  
  private void testRandomTrieAndClassicRangeQuery(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    int totalTermCountT=0,totalTermCountC=0,termCountT,termCountC;
    int num = TestUtil.nextInt(random(), 10, 20);
    for (int i = 0; i < num; i++) {
      long lower=(long)(random().nextDouble()*noDocs*distance)+startOffset;
      long upper=(long)(random().nextDouble()*noDocs*distance)+startOffset;
      if (lower>upper) {
        long a=lower; lower=upper; upper=a;
      }
      final BytesRef lowerBytes = new BytesRef(BigNumericUtils.getBufferSize(valueSize)), upperBytes = new BytesRef(BigNumericUtils.getBufferSize(valueSize));
      BigNumericUtils.bigIntegerToPrefixCodedBytes(BigInteger.valueOf(lower), 0, lowerBytes, valueSize);
      BigNumericUtils.bigIntegerToPrefixCodedBytes(BigInteger.valueOf(upper), 0, upperBytes, valueSize);
      
      // test inclusive range
      NumericRangeQuery<BigInteger> tq=NumericRangeQuery.newBigIntegerRange(field, precisionStep, BigInteger.valueOf(lower), BigInteger.valueOf(upper), true, true, valueSize);
      TermRangeQuery cq=new TermRangeQuery(field, lowerBytes, upperBytes, true, true);
      TopDocs tTopDocs = searcher.search(tq, 1);
      TopDocs cTopDocs = searcher.search(cq, 1);
      assertEquals("Returned count for NumericRangeQuery and TermRangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      totalTermCountT += termCountT = countTerms(tq);
      totalTermCountC += termCountC = countTerms(cq);
      checkTermCounts(precisionStep, termCountT, termCountC);
      // test exclusive range
      tq=NumericRangeQuery.newBigIntegerRange(field, precisionStep, BigInteger.valueOf(lower), BigInteger.valueOf(upper), false, false, valueSize);
      cq=new TermRangeQuery(field, lowerBytes, upperBytes, false, false);
      tTopDocs = searcher.search(tq, 1);
      cTopDocs = searcher.search(cq, 1);
      assertEquals("Returned count for NumericRangeQuery and TermRangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      totalTermCountT += termCountT = countTerms(tq);
      totalTermCountC += termCountC = countTerms(cq);
      checkTermCounts(precisionStep, termCountT, termCountC);
      // test left exclusive range
      tq=NumericRangeQuery.newBigIntegerRange(field, precisionStep, BigInteger.valueOf(lower), BigInteger.valueOf(upper), false, true, valueSize);
      cq=new TermRangeQuery(field, lowerBytes, upperBytes, false, true);
      tTopDocs = searcher.search(tq, 1);
      cTopDocs = searcher.search(cq, 1);
      assertEquals("Returned count for NumericRangeQuery and TermRangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      totalTermCountT += termCountT = countTerms(tq);
      totalTermCountC += termCountC = countTerms(cq);
      checkTermCounts(precisionStep, termCountT, termCountC);
      // test right exclusive range
      tq=NumericRangeQuery.newBigIntegerRange(field, precisionStep, BigInteger.valueOf(lower), BigInteger.valueOf(upper), true, false, valueSize);
      cq=new TermRangeQuery(field, lowerBytes, upperBytes, true, false);
      tTopDocs = searcher.search(tq, 1);
      cTopDocs = searcher.search(cq, 1);
      assertEquals("Returned count for NumericRangeQuery and TermRangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      totalTermCountT += termCountT = countTerms(tq);
      totalTermCountC += termCountC = countTerms(cq);
      checkTermCounts(precisionStep, termCountT, termCountC);
    }
    
    checkTermCounts(precisionStep, totalTermCountT, totalTermCountC);
    if (VERBOSE && precisionStep != Integer.MAX_VALUE) {
      System.out.println("Average number of terms during random search on '" + field + "':");
      System.out.println(" Numeric query: " + (((double)totalTermCountT)/(num * 4)));
      System.out.println(" Classical query: " + (((double)totalTermCountC)/(num * 4)));
    }
  }
  
  @Test
  public void testEmptyEnums() throws Exception {
    int count=3000;
    long lower=(distance*3/2)+startOffset, upper=lower + count*distance + (distance/3);
    // test empty enum
    assert lower < upper;
    assertTrue(0 < countTerms(NumericRangeQuery.newBigIntegerRange("field4", 4, BigInteger.valueOf(lower), BigInteger.valueOf(upper), true, true, valueSize)));
    assertEquals(0, countTerms(NumericRangeQuery.newBigIntegerRange("field4", 4, BigInteger.valueOf(upper), BigInteger.valueOf(lower), true, true, valueSize)));
    // test empty enum outside of bounds
    lower = distance*noDocs+startOffset;
    upper = 2L * lower;
    assert lower < upper;
    assertEquals(0, countTerms(NumericRangeQuery.newBigIntegerRange("field4", 4, BigInteger.valueOf(lower), BigInteger.valueOf(upper), true, true, valueSize)));
  }
  
  private int countTerms(MultiTermQuery q) throws Exception {
    final Terms terms = MultiFields.getTerms(reader, q.getField());
    if (terms == null)
      return 0;
    final TermsEnum termEnum = q.getTermsEnum(terms);
    assertNotNull(termEnum);
    int count = 0;
    BytesRef cur, last = null;
    while ((cur = termEnum.next()) != null) {
      count++;
      if (last != null) {
        assertTrue(last.compareTo(cur) < 0);
      }
      last = BytesRef.deepCopyOf(cur);
    } 
    // LUCENE-3314: the results after next() already returned null are undefined,
    // assertNull(termEnum.next());
    return count;
  }
  
  private void checkTermCounts(int precisionStep, int termCountT, int termCountC) {
    if (precisionStep == Integer.MAX_VALUE) {
      assertEquals("Number of terms should be equal for unlimited precStep", termCountC, termCountT);
    } else {
      assertTrue("Number of terms for NRQ should be <= compared to classical TRQ", termCountT <= termCountC);
    }
  }

  @Test
  public void testRandomTrieAndClassicRangeQuery_8bit() throws Exception {
    testRandomTrieAndClassicRangeQuery(8);
  }
  
  @Test
  public void testRandomTrieAndClassicRangeQuery_6bit() throws Exception {
    testRandomTrieAndClassicRangeQuery(6);
  }
  
  @Test
  public void testRandomTrieAndClassicRangeQuery_4bit() throws Exception {
    testRandomTrieAndClassicRangeQuery(4);
  }
  
  @Test
  public void testRandomTrieAndClassicRangeQuery_2bit() throws Exception {
    testRandomTrieAndClassicRangeQuery(2);
  }
  
  @Test
  public void testRandomTrieAndClassicRangeQuery_NoTrie() throws Exception {
    testRandomTrieAndClassicRangeQuery(Integer.MAX_VALUE);
  }
  
  private void testRangeSplit(int precisionStep) throws Exception {
    String field="ascfield"+precisionStep;
    // 10 random tests
    int num = TestUtil.nextInt(random(), 10, 20);
    for (int i = 0; i < num; i++) {
      long lower=(long)(random().nextDouble()*noDocs - noDocs/2);
      long upper=(long)(random().nextDouble()*noDocs - noDocs/2);
      if (lower>upper) {
        long a=lower; lower=upper; upper=a;
      }
      // test inclusive range
      Query tq=NumericRangeQuery.newBigIntegerRange(field, precisionStep, BigInteger.valueOf(lower), BigInteger.valueOf(upper), true, true, valueSize);
      TopDocs tTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count of range query must be equal to inclusive range length", upper-lower+1, tTopDocs.totalHits );
      // test exclusive range
      tq=NumericRangeQuery.newBigIntegerRange(field, precisionStep, BigInteger.valueOf(lower), BigInteger.valueOf(upper), false, false, valueSize);
      tTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count of range query must be equal to exclusive range length", Math.max(upper-lower-1, 0), tTopDocs.totalHits );
      // test left exclusive range
      tq=NumericRangeQuery.newBigIntegerRange(field, precisionStep, BigInteger.valueOf(lower), BigInteger.valueOf(upper), false, true, valueSize);
      tTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count of range query must be equal to half exclusive range length", upper-lower, tTopDocs.totalHits );
      // test right exclusive range
      tq=NumericRangeQuery.newBigIntegerRange(field, precisionStep, BigInteger.valueOf(lower), BigInteger.valueOf(upper), true, false, valueSize);
      tTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count of range query must be equal to half exclusive range length", upper-lower, tTopDocs.totalHits );
    }
  }

  @Test
  public void testRangeSplit_8bit() throws Exception {
    testRangeSplit(8);
  }
  
  @Test
  public void testRangeSplit_6bit() throws Exception {
    testRangeSplit(6);
  }
  
  @Test
  public void testRangeSplit_4bit() throws Exception {
    testRangeSplit(4);
  }
  
  @Test
  public void testRangeSplit_2bit() throws Exception {
    testRangeSplit(2);
  }

  
  private void testSorting(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    // 10 random tests, the index order is ascending,
    // so using a reverse sort field should retun descending documents
    int num = TestUtil.nextInt(random(), 10, 20);
    for (int i = 0; i < num; i++) {
      long lower=(long)(random().nextDouble()*noDocs*distance)+startOffset;
      long upper=(long)(random().nextDouble()*noDocs*distance)+startOffset;
      if (lower>upper) {
        long a=lower; lower=upper; upper=a;
      }
      Query tq=NumericRangeQuery.newBigIntegerRange(field, precisionStep, BigInteger.valueOf(lower), BigInteger.valueOf(upper), true, true, valueSize);
      TopDocs topDocs = searcher.search(tq, null, noDocs, new Sort(new SortField(field, SortField.Type.BIG_INTEGER, true)));
      if (topDocs.totalHits==0) continue;
      ScoreDoc[] sd = topDocs.scoreDocs;
      assertNotNull(sd);
      long last = new BigInteger(searcher.doc(sd[0].doc).getField(field).stringValue()).longValue();
      for (int j=1; j<sd.length; j++) {
        long act=new BigInteger(searcher.doc(sd[j].doc).getField(field).stringValue()).longValue();
        assertTrue("Docs should be sorted backwards", last>act );
        last=act;
      }
    }
  }

  @Test
  public void testSorting_8bit() throws Exception {
    testSorting(8);
  }
  
  @Test
  public void testSorting_6bit() throws Exception {
    testSorting(6);
  }
  
  @Test
  public void testSorting_4bit() throws Exception {
    testSorting(4);
  }
  
  @Test
  public void testSorting_2bit() throws Exception {
    testSorting(2);
  }
  
  @Test
  public void testEqualsAndHash() throws Exception {
    QueryUtils.checkHashEquals(NumericRangeQuery.newBigIntegerRange("test1", 4, BigInteger.valueOf(10L), BigInteger.valueOf(20L), true, true, valueSize));
    QueryUtils.checkHashEquals(NumericRangeQuery.newBigIntegerRange("test2", 4, BigInteger.valueOf(10L), BigInteger.valueOf(20L), false, true, valueSize));
    QueryUtils.checkHashEquals(NumericRangeQuery.newBigIntegerRange("test3", 4, BigInteger.valueOf(10L), BigInteger.valueOf(20L), true, false, valueSize));
    QueryUtils.checkHashEquals(NumericRangeQuery.newBigIntegerRange("test4", 4, BigInteger.valueOf(10L), BigInteger.valueOf(20L), false, false, valueSize));
    QueryUtils.checkHashEquals(NumericRangeQuery.newBigIntegerRange("test5", 4, BigInteger.valueOf(10L), null, true, true, valueSize));
    QueryUtils.checkHashEquals(NumericRangeQuery.newBigIntegerRange("test6", 4, null, BigInteger.valueOf(20L), true, true, valueSize));
    QueryUtils.checkHashEquals(NumericRangeQuery.newBigIntegerRange("test7", 4, null, null, true, true, valueSize));
    QueryUtils.checkEqual(
        NumericRangeQuery.newBigIntegerRange("test8", 4, BigInteger.valueOf(10L), BigInteger.valueOf(20L), true, true, valueSize),
        NumericRangeQuery.newBigIntegerRange("test8", 4, BigInteger.valueOf(10L), BigInteger.valueOf(20L), true, true, valueSize)
    );
    QueryUtils.checkUnequal(
        NumericRangeQuery.newBigIntegerRange("test9", 4, BigInteger.valueOf(10L), BigInteger.valueOf(20L), true, true, valueSize),
        NumericRangeQuery.newBigIntegerRange("test9", 8, BigInteger.valueOf(10L), BigInteger.valueOf(20L), true, true, valueSize)
    );
    QueryUtils.checkUnequal(
        NumericRangeQuery.newBigIntegerRange("test10a", 4, BigInteger.valueOf(10L), BigInteger.valueOf(20L), true, true, valueSize),
        NumericRangeQuery.newBigIntegerRange("test10b", 4, BigInteger.valueOf(10L), BigInteger.valueOf(20L), true, true, valueSize)
    );
    QueryUtils.checkUnequal(
        NumericRangeQuery.newBigIntegerRange("test11", 4, BigInteger.valueOf(10L), BigInteger.valueOf(20L), true, true, valueSize),
        NumericRangeQuery.newBigIntegerRange("test11", 4, BigInteger.valueOf(20L), BigInteger.valueOf(10L), true, true, valueSize)
    );
    QueryUtils.checkUnequal(
        NumericRangeQuery.newBigIntegerRange("test12", 4, BigInteger.valueOf(10L), BigInteger.valueOf(20L), true, true, valueSize),
        NumericRangeQuery.newBigIntegerRange("test12", 4, BigInteger.valueOf(10L), BigInteger.valueOf(20L), false, true, valueSize)
    );
  }
}
