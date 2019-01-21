/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.util;

import java.lang.invoke.MethodHandles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaosong
 * @since 2019/1/21
 */
public class CallStack {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static void printCallStatck() {
    Throwable ex = new Throwable();
    StackTraceElement[] stackElements = ex.getStackTrace();
    LOG.info("                               ");
    if (stackElements != null) {
      for (int i = 0; i < stackElements.length; i++) {
        LOG.info(stackElements[i].getClassName()+" "
        +stackElements[i].getFileName()+" "
        +stackElements[i].getLineNumber()+" "
        +stackElements[i].getMethodName());
        LOG.info("-----------------------------------");
      }
    }
  }
}
