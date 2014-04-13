package org.apache.lucene.document;

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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.util.BytesRef;

import java.math.BigInteger;

/**
 * todo: javadoc
 */
public class BigNumericDocValuesField extends Field {

  /**
   * Type for numeric DocValues.
   */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDocValueType(FieldInfo.DocValuesType.BINARY);
    TYPE.freeze();
  }

  /**
   * Creates a new DocValues field with the specified 64-bit long value
   * @param name field name
   * @param value 64-bit long value
   * @throws IllegalArgumentException if the field name is null
   */
  public BigNumericDocValuesField(String name, BigInteger value) {
    super(name, TYPE);
    fieldsData = new BytesRef(value.toByteArray());
  }
}
