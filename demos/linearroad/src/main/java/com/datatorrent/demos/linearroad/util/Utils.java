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
package com.datatorrent.demos.linearroad.util;

import com.datatorrent.demos.linearroad.data.PositionReport;

public class Utils
{
  public static boolean isTravelLane(PositionReport tuple)
  {
    if (tuple.getLane() == 4 || tuple.getLane() == 0) {
      return false;
    }
    return true;
  }

  public static boolean isExitLane(PositionReport tuple)
  {
    if (tuple.getLane() == 4) {
      return true;
    }
    return false;
  }

  public static int getMinute(int seconds)
  {
    return ((seconds / 60) + 1);
  }

  public static int getSegment(int position)
  {
    return position / 5280;
  }
}
