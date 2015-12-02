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

public class RuleViolation<RULE_KEY, KEY>
{
  private RULE_KEY ruleKey;
  private KEY key;
  private String message;

  public RuleViolation(RULE_KEY ruleKey, KEY key, String message)
  {
    this.ruleKey = Preconditions.checkNotNull(ruleKey);
    this.key = Preconditions.checkNotNull(key);
    this.message = Preconditions.checkNotNull(message);
  }

  public RULE_KEY getRuleKey()
  {
    return ruleKey;
  }

  public KEY getKey()
  {
    return key;
  }

  public String getMessage()
  {
    return message;
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 37 * hash + Objects.hashCode(this.ruleKey);
    hash = 37 * hash + Objects.hashCode(this.key);
    hash = 37 * hash + Objects.hashCode(this.message);
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
    final RuleViolation<?, ?> other = (RuleViolation<?, ?>)obj;
    if (!Objects.equals(this.ruleKey, other.ruleKey)) {
      return false;
    }
    if (!Objects.equals(this.key, other.key)) {
      return false;
    }
    if (!Objects.equals(this.message, other.message)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString()
  {
    return "RuleViolation{" + "ruleKey=" + ruleKey + ", key=" + key + ", message=" + message + '}';
  }
}
