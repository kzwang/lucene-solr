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

import org.apache.lucene.index.FieldInfo.IndexOptions;

import java.math.BigInteger;

/**
 *
 */
public final class BigIntegerField extends Field {

  /**
   * Type for a BigIntegerField that is not stored:
   * normalization factors, frequencies, and positions are omitted.
   */
  public static final FieldType TYPE_NOT_STORED = new FieldType();
  static {
    TYPE_NOT_STORED.setIndexed(true);
    TYPE_NOT_STORED.setTokenized(true);
    TYPE_NOT_STORED.setOmitNorms(true);
    TYPE_NOT_STORED.setIndexOptions(IndexOptions.DOCS_ONLY);
    TYPE_NOT_STORED.setNumericType(FieldType.NumericType.BIG_INTEGER);
    TYPE_NOT_STORED.freeze();
  }

  /**
   * Type for a stored BigIntegerField:
   * normalization factors, frequencies, and positions are omitted.
   */
  public static final FieldType TYPE_STORED = new FieldType();
  static {
    TYPE_STORED.setIndexed(true);
    TYPE_STORED.setTokenized(true);
    TYPE_STORED.setOmitNorms(true);
    TYPE_STORED.setIndexOptions(IndexOptions.DOCS_ONLY);
    TYPE_STORED.setNumericType(FieldType.NumericType.BIG_INTEGER);
    TYPE_STORED.setStored(true);
    TYPE_STORED.freeze();
  }

  /** Creates a stored or un-stored LongField with the provided value
   *  and default <code>precisionStep</code> {@link
   *  org.apache.lucene.util.NumericUtils#PRECISION_STEP_DEFAULT} (4).
   *  @param name field name
   *  @param value 64-bit long value
   *  @param stored Store.YES if the content should also be stored
   *  @throws IllegalArgumentException if the field name is null.
   */
  public BigIntegerField(String name, BigInteger value, Store stored, int valueSize) {
    super(name, stored == Store.YES ? TYPE_STORED : TYPE_NOT_STORED);
    fieldsData = value;
    this.valueSize = valueSize;
  }

  /** Expert: allows you to customize the {@link
   *  org.apache.lucene.document.FieldType}.
   *  @param name field name
   *  @param value 64-bit long value
   *  @param type customized field type: must have {@link org.apache.lucene.document.FieldType#numericType()}
   *         of {@link org.apache.lucene.document.FieldType.NumericType#LONG}.
   *  @throws IllegalArgumentException if the field name or type is null, or
   *          if the field type does not have a LONG numericType()
   */
  public BigIntegerField(String name, BigInteger value, FieldType type, int valueSize) {
    super(name, type);
    if (type.numericType() != FieldType.NumericType.BIG_INTEGER) {
      throw new IllegalArgumentException("type.numericType() must be BIG_INTEGER but got " + type.numericType());
    }
    fieldsData = value;
    this.valueSize = valueSize;
  }
}
