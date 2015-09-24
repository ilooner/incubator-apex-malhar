/*
 * Copyright (c) 2015 DataTorrent
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

package com.datatorrent.lib.appdata.props;

import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;

public class BeanPropertyUpdaterTest
{
  public static class TestObject
  {
    private long longVal;
    private double doubleVal;
    private String stringVal;

    public TestObject()
    {
    }

    public TestObject(long longVal, double doubleVal, String stringVal)
    {
      this.longVal = longVal;
      this.doubleVal = doubleVal;
      this.stringVal = stringVal;
    }

    /**
     * @return the longVal
     */
    public long getLongVal()
    {
      return longVal;
    }

    /**
     * @param longVal the longVal to set
     */
    public void setLongVal(long longVal)
    {
      this.longVal = longVal;
    }

    /**
     * @return the doubleVal
     */
    public double getDoubleVal()
    {
      return doubleVal;
    }

    /**
     * @param doubleVal the doubleVal to set
     */
    public void setDoubleVal(double doubleVal)
    {
      this.doubleVal = doubleVal;
    }

    /**
     * @return the stringVal
     */
    public String getStringVal()
    {
      return stringVal;
    }

    /**
     * @param stringVal the stringVal to set
     */
    public void setStringVal(String stringVal)
    {
      this.stringVal = stringVal;
    }

    @Override
    public int hashCode()
    {
      int hash = 7;
      hash = 79 * hash + (int)(this.longVal ^ (this.longVal >>> 32));
      hash = 79 * hash + (int)(Double.doubleToLongBits(this.doubleVal) ^ (Double.doubleToLongBits(this.doubleVal) >>> 32));
      hash = 79 * hash + Objects.hashCode(this.stringVal);
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
      final TestObject other = (TestObject)obj;
      if (this.longVal != other.longVal) {
        return false;
      }
      if (Double.doubleToLongBits(this.doubleVal) != Double.doubleToLongBits(other.doubleVal)) {
        return false;
      }
      if (!Objects.equals(this.stringVal, other.stringVal)) {
        return false;
      }
      return true;
    }
  }

  @Test
  public void simpleTest()
  {
    TestObject testObject = new TestObject();
    BeanPropertyUpdater<TestObject> beanPropertyUpdater = new BeanPropertyUpdater<>(testObject);

    beanPropertyUpdater.setup(null);

    beanPropertyUpdater.beginWindow(0L);
    beanPropertyUpdater.registerUpdate(new PropertyUpdate("longVal", "13"));
    beanPropertyUpdater.registerUpdate(new PropertyUpdate("doubleVal", "5.5"));
    beanPropertyUpdater.registerUpdate(new PropertyUpdate("stringVal", "abc"));
    beanPropertyUpdater.endWindow();

    beanPropertyUpdater.beginWindow(0L);
    beanPropertyUpdater.endWindow();

    beanPropertyUpdater.teardown();

    TestObject expectedObject = new TestObject(13L, 5.5, "abc");

    Assert.assertEquals(expectedObject, testObject);
  }
}
