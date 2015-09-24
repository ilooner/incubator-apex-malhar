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

import java.lang.reflect.InvocationTargetException;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import org.apache.commons.beanutils.BeanUtils;

import com.datatorrent.api.Context.OperatorContext;

public class BeanPropertyUpdater<T> extends AbstractPropertyUpdater
{
  private T updateDestination;

  public BeanPropertyUpdater(T updateDestination)
  {
    this.updateDestination = updateDestination;
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  protected void applyUpdates(List<PropertyUpdate> propertyUpdates)
  {
    Map<String, String> properties = Maps.newHashMap();

    for (PropertyUpdate propertyUpdate : propertyUpdates) {
      properties.put(propertyUpdate.getName(), propertyUpdate.getValue());
    }

    try {
      BeanUtils.populate(updateDestination, properties);
    } catch (IllegalAccessException | InvocationTargetException ex) {
      throw new RuntimeException(ex);
    }
  }
}
