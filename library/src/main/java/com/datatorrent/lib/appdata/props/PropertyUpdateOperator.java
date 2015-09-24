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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.logging.Level;
import java.util.logging.Logger;

public class PropertyUpdateOperator implements Operator
{
  public static final String FIELD_PROPERTY_NAME = "name";
  public static final String FIELD_PROPERTY_VALUE = "value";

  public final transient DefaultOutputPort<PropertyUpdate> propertyUpdate = new DefaultOutputPort<>();

  private String updateProperty;

  public PropertyUpdateOperator()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void teardown()
  {
  }

  /**
   * @return the updateProperty
   */
  public String getUpdateProperty()
  {
    return updateProperty;
  }

  /**
   * @param updateProperty the updateProperty to set
   */
  public void setUpdateProperty(String updateProperty)
  {
    try {
      JSONObject jo = new JSONObject(updateProperty);
      String name = jo.getString(FIELD_PROPERTY_NAME);
      String value = jo.getString(FIELD_PROPERTY_VALUE);

      propertyUpdate.emit(new PropertyUpdate(name, value));
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
  }
}
