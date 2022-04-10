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
package com.alipay.sofa.registry.task;

import com.alipay.sofa.registry.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class BlockingQueuesTest {
  @Test
  public void test() {
    BlockingQueues queues = new BlockingQueues(2, 2, true);
    Assert.assertEquals(queues.queueNum(), 2);
    Assert.assertEquals(queues.getTotalQueueSize(), 0);
    // avgSize=1
    // size<avg
    queues.put(0, new Object());
    // totalSize<buffer
    queues.put(0, new Object());
    TestUtils.assertException(
        FastRejectedExecutionException.class, () -> queues.put(0, new Object()));

    // size<avg
    queues.put(1, new Object());
    TestUtils.assertException(
        FastRejectedExecutionException.class, () -> queues.put(1, new Object()));
    Assert.assertEquals(queues.getTotalQueueSize(), 3);
    Assert.assertEquals(queues.getQueue(0).size(), 2);
    Assert.assertEquals(queues.getQueue(1).size(), 1);
  }
}
