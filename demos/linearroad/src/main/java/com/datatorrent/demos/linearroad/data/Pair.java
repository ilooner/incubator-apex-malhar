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
package com.datatorrent.demos.linearroad.data;

public class Pair
{

  public int left;
  public int right;

  public Pair()
  {
    this(0, 0);
  }

  public Pair(int left, int right)
  {
    this.left = left;
    this.right = right;
  }

  @Override
  public String toString()
  {
    return "Pair{" +
      "totalCars=" + left +
      ", totalSpeed=" + right +
      '}';
  }
}
