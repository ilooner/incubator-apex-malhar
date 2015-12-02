/*
 * Copyright 2015 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.lib.rules;

import com.google.common.base.Preconditions;
import java.util.Objects;

public class Rule<KEY>
{
  private KEY key;
  private String expression;

  public Rule(KEY key, String expression, String violationMessage)
  {
    this.key = Preconditions.checkNotNull(key);
    this.expression = Preconditions.checkNotNull(expression);
  }

  public KEY getKey()
  {
    return key;
  }

  public String getExpression()
  {
    return expression;
  }

  @Override
  public int hashCode()
  {
    int hash = 5;
    hash = 67 * hash + Objects.hashCode(this.key);
    hash = 67 * hash + Objects.hashCode(this.expression);
    return hash;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Rule<?> other = (Rule<?>)obj;
    if (!Objects.equals(this.key, other.key)) {
      return false;
    }
    if (!Objects.equals(this.expression, other.expression)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString()
  {
    return "Rule{" + "key=" + key + ", expression=" + expression + '}';
  }
}
