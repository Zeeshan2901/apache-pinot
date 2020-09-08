/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.aggregation.function;

import java.util.Map;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.DoubleAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.StringAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.StringGroupByResultHolder;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.spi.data.FieldSpec;


public class MaxAggregationFunction extends BaseSingleInputAggregationFunction<String, String> {
  private static final String DEFAULT_INITIAL_VALUE = null;

  public MaxAggregationFunction(ExpressionContext expression) {
    super(expression);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MAX;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new StringAggregationResultHolder(DEFAULT_INITIAL_VALUE);
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new StringGroupByResultHolder(initialCapacity, maxCapacity, DEFAULT_INITIAL_VALUE);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[] valueArray = blockValSetMap.get(_expression).getDoubleValuesSV();
    double max = aggregationResultHolder.getDoubleResult();
    for (int i = 0; i < length; i++) {
      double value = valueArray[i];
      if (value > max) {
        max = value;
      }
    }
    aggregationResultHolder.setValue(max);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    FieldSpec.DataType dataType = blockValSetMap.get(_expression).getValueType();
    if (dataType.isNumeric()) {
      double[] valueArray = blockValSetMap.get(_expression).getDoubleValuesSV();
      for (int i = 0; i < length; i++) {
        double value = valueArray[i];
        int groupKey = groupKeyArray[i];
        if (value > groupByResultHolder.getDoubleResult(groupKey)) {
          groupByResultHolder.setValueForKey(groupKey, value);
        }
      }
    } else {
      String[] valueArray = blockValSetMap.get(_expression).getStringValuesSV();
      for (int i = 0; i < length; i++) {
        String value = valueArray[i];
        int groupKey = groupKeyArray[i];
        String compare = groupByResultHolder.getStringResult(groupKey);
        if(compare == null){
          compare="";
        }
        if(value==null){
          value="";
        }
        if (value.compareTo(compare) > 0) {
          groupByResultHolder.setValueForKey(groupKey, value);
        }
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[] valueArray = blockValSetMap.get(_expression).getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      double value = valueArray[i];
      for (int groupKey : groupKeysArray[i]) {
        if (value > groupByResultHolder.getDoubleResult(groupKey)) {
          groupByResultHolder.setValueForKey(groupKey, value);
        }
      }
    }
  }

  @Override
  public String extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getStringResult();
  }

  @Override
  public String extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getStringResult(groupKey);
  }

  @Override
  public String merge(String intermediateResult1, String intermediateResult2) {
    if (intermediateResult1.compareTo(intermediateResult2) > 0 ) {
      return intermediateResult1;
    } else {
      return intermediateResult2;
    }
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return true;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public String extractFinalResult(String intermediateResult) {
    return intermediateResult;
  }
}
