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

package com.datatorrent.lib.appdata.schemas;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.text.DecimalFormat;
import javax.validation.constraints.NotNull;

public class AppDataFormatter implements Serializable
{
  private String floatFormatString;
  private String doubleFormatString;

  private String byteFormatString;
  private String shortFormatString;
  private String intFormatString;
  private String longFormatString;

  private String discreteFormatString;
  private String continuousFormatString;

  private transient DecimalFormat floatFormat;
  private transient DecimalFormat doubleFormat;
  private transient DecimalFormat byteFormat;
  private transient DecimalFormat shortFormat;
  private transient DecimalFormat intFormat;
  private transient DecimalFormat longFormat;

  public AppDataFormatter()
  {
  }

  public String format(Object object)
  {
    Type type = Type.CLASS_TO_TYPE.get(object.getClass());

    if(type == null) {
      return object.toString();
    }

    switch(type) {
      case FLOAT:
      {
        return format((Float) object);
      }
      case DOUBLE:
      {
        return format((Double) object);
      }
      case BYTE:
      {
        return format((Byte) object);
      }
      case SHORT:
      {
        return format((Short) object);
      }
      case INTEGER:
      {
        return format((Integer) object);
      }
      case LONG:
      {
        return format((Long) object);
      }
      default:
        return object.toString();
    }
  }

  public String format(float val)
  {
    DecimalFormat df = getFloatFormat();

    if(df != null) {
      return df.format(val);
    }

    return Float.toString(val);
  }

  public String format(double val)
  {
    DecimalFormat df = getDoubleFormat();

    if(df != null) {
      return df.format(val);
    }

    return Double.toString(val);
  }

  public String format(byte val)
  {
    DecimalFormat df = getByteFormat();

    if(df != null) {
      return df.format(val);
    }

    return Byte.toString(val);
  }

  public String format(short val)
  {
    DecimalFormat df = getShortFormat();

    if(df != null) {
      return df.format(val);
    }

    return Short.toString(val);
  }

  public String format(int val)
  {
    DecimalFormat df = getIntFormat();

    if(df != null) {
      return df.format(val);
    }

    return Integer.toString(val);
  }

  public String format(long val)
  {
    DecimalFormat df = getLongFormat();

    if(df != null) {
      return df.format(val);
    }

    return Long.toString(val);
  }

  public DecimalFormat getFloatFormat()
  {
    if(floatFormat == null && getFloatFormatString() != null) {
      floatFormat = new DecimalFormat(floatFormatString);
    }

    return floatFormat;
  }

  /**
   * @return the doubleFormat
   */
  public DecimalFormat getDoubleFormat()
  {
    if(doubleFormat == null && getFloatFormatString() != null) {
      doubleFormat = new DecimalFormat(doubleFormatString);
    }

    return doubleFormat;
  }

  /**
   * @return the byteFormat
   */
  public DecimalFormat getByteFormat()
  {
    if(byteFormat == null && getFloatFormatString() != null) {
      byteFormat = new DecimalFormat(byteFormatString);
    }

    return byteFormat;
  }

  /**
   * @return the shortFormat
   */
  public DecimalFormat getShortFormat()
  {
    if(shortFormat == null && getFloatFormatString() != null) {
      shortFormat = new DecimalFormat(shortFormatString);
    }

    return shortFormat;
  }

  public DecimalFormat getIntFormat()
  {
    if(intFormat == null && getIntFormatString() != null) {
      intFormat = new DecimalFormat(intFormatString);
    }

    return intFormat;
  }

  /**
   * @return the longFormat
   */
  public DecimalFormat getLongFormat()
  {
    if(longFormat == null && getFloatFormatString() != null) {
      longFormat = new DecimalFormat(longFormatString);
    }

    return longFormat;
  }

  public String getDiscreteFormatString()
  {
    return discreteFormatString;
  }

  public void setDiscreteFormatString(@NotNull String discreteFormatString)
  {
    this.discreteFormatString = Preconditions.checkNotNull(discreteFormatString);
    this.byteFormatString = discreteFormatString;
    this.shortFormatString = discreteFormatString;
    this.intFormatString = discreteFormatString;
    this.longFormatString = discreteFormatString;
  }

  public String getContinuousFormatString()
  {
    return continuousFormatString;
  }

  public void setContinuousFormatString(@NotNull String continuousFormatString)
  {
    this.continuousFormatString = Preconditions.checkNotNull(continuousFormatString);
    this.floatFormatString = continuousFormatString;
    this.doubleFormatString = continuousFormatString;
  }

  /**
   * @return the decimalFormatString
   */
  public String getFloatFormatString()
  {
    return floatFormatString;
  }

  /**
   * @param decimalFormatString the decimalFormatString to set
   */
  public void setFloatFormatString(@NotNull String decimalFormatString)
  {
    this.floatFormatString = Preconditions.checkNotNull(decimalFormatString);
  }

  /**
   * @return the doubleFormatString
   */
  public String getDoubleFormatString()
  {
    return doubleFormatString;
  }

  /**
   * @param doubleFormatString the doubleFormatString to set
   */
  public void setDoubleFormatString(@NotNull String doubleFormatString)
  {
    this.doubleFormatString = Preconditions.checkNotNull(doubleFormatString);
  }

  /**
   * @return the intFormatString
   */
  public String getIntFormatString()
  {
    return intFormatString;
  }

  /**
   * @param intFormatString the intFormatString to set
   */
  public void setIntFormatString(@NotNull String intFormatString)
  {
    this.intFormatString = Preconditions.checkNotNull(intFormatString);
  }

  /**
   * @return the byteFormatString
   */
  public String getByteFormatString()
  {
    return byteFormatString;
  }

  /**
   * @param byteFormatString the byteFormatString to set
   */
  public void setByteFormatString(@NotNull String byteFormatString)
  {
    this.byteFormatString = Preconditions.checkNotNull(byteFormatString);
  }

  /**
   * @return the shortFormatString
   */
  public String getShortFormatString()
  {
    return shortFormatString;
  }

  /**
   * @param shortFormatString the shortFormatString to set
   */
  public void setShortFormatString(@NotNull String shortFormatString)
  {
    this.shortFormatString = Preconditions.checkNotNull(shortFormatString);
  }

  /**
   * @return the longFormatString
   */
  public String getLongFormatString()
  {
    return longFormatString;
  }

  /**
   * @param longFormatString the longFormatString to set
   */
  public void setLongFormatString(@NotNull String longFormatString)
  {
    this.longFormatString = Preconditions.checkNotNull(longFormatString);
  }
}