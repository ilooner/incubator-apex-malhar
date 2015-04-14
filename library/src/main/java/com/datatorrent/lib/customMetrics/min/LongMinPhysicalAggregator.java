/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.customMetrics.min;

import java.io.Serializable;

import com.datatorrent.api.CustomMetrics;
import com.datatorrent.api.annotation.Name;

@Name("Min")
public class LongMinPhysicalAggregator implements CustomMetrics.PhysicalAggregator<Long>, Serializable
{
  @Override
  public Long aggregate(CustomMetrics.PhysicalMetrics physicalMetrics)
  {
    Long min = null;
    for (Number metric : physicalMetrics.<Number>getValues()) {
      long value = metric.longValue();
      if (min == null || value < min) {
        min = value;
      }
    }
    return min;
  }

  private static final long serialVersionUID = 201504081319L;
}
