package org.apache.lucene.queryParser.core.builders;

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

import junit.framework.Assert;

import org.apache.lucene.queryParser.core.QueryNodeException;
import org.apache.lucene.queryParser.core.builders.QueryBuilder;
import org.apache.lucene.queryParser.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryParser.core.nodes.FieldQueryNode;
import org.apache.lucene.queryParser.core.nodes.QueryNode;
import org.apache.lucene.queryParser.core.nodes.QueryNodeImpl;
import org.apache.lucene.queryParser.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryParser.core.util.UnescapedCharSequence;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestQueryTreeBuilder extends LuceneTestCase {
  
  @Test
  public void testSetFieldBuilder() throws QueryNodeException {
    QueryTreeBuilder qtb = new QueryTreeBuilder();
    qtb.setBuilder("field", new DummyBuilder());
    Object result = qtb.build(new FieldQueryNode(new UnescapedCharSequence("field"), "foo", 0, 0));
    Assert.assertEquals("OK", result);
    
    // LUCENE-4890
    qtb = new QueryTreeBuilder();
    qtb.setBuilder(DummyQueryNodeInterface.class, new DummyBuilder());
    result = qtb.build(new DummyQueryNode());
    Assert.assertEquals("OK", result);
  }
  
  private static interface DummyQueryNodeInterface extends QueryNode {
    
  }
  
  private static abstract class AbstractDummyQueryNode extends QueryNodeImpl implements DummyQueryNodeInterface {
    
  }
  
  private static class DummyQueryNode extends AbstractDummyQueryNode {

    public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
      return "DummyQueryNode";
    }
    
  }
  
  private static class DummyBuilder implements QueryBuilder {

    public Object build(QueryNode queryNode) throws QueryNodeException {
      return "OK";
    }
    
  }

}
