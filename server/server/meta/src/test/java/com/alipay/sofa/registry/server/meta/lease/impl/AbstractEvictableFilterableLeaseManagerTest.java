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
package com.alipay.sofa.registry.server.meta.lease.impl;

import com.alipay.sofa.registry.common.model.metaserver.Lease;
import com.alipay.sofa.registry.server.meta.AbstractMetaServerTestBase;
import com.alipay.sofa.registry.server.meta.lease.LeaseFilter;
import com.alipay.sofa.registry.server.meta.lease.filter.DefaultForbiddenServerManager;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AbstractEvictableFilterableLeaseManagerTest extends AbstractMetaServerTestBase {

  private AbstractEvictableFilterableLeaseManager leaseManager;

  private DefaultForbiddenServerManager forbiddenServerManager =
      new DefaultForbiddenServerManager(new InMemoryProvideDataRepo());

  @Before
  public void beforeAbstractEvictableFilterableLeaseManagerTest()
      throws TimeoutException, InterruptedException {
    leaseManager =
        new AbstractEvictableFilterableLeaseManager() {
          @Override
          protected int getEvictBetweenMilli() {
            return 10;
          }

          @Override
          protected int getIntervalMilli() {
            return 1000;
          }
        };
    leaseManager.metaLeaderService = metaLeaderService;
    leaseManager.setLeaseFilters(Lists.newArrayList(forbiddenServerManager));
    makeMetaLeader();
  }

  @Test
  public void testGetLeaseMeta() {
    int nodeNum = 100;
    for (int i = 0; i < nodeNum; i++) {
      leaseManager.renew(new SimpleNode(randomIp()), 100);
    }
    SimpleNode node = new SimpleNode(randomIp());
    leaseManager.renew(node, 100);
    Assert.assertEquals(nodeNum + 1, leaseManager.getLeaseMeta().getClusterMembers().size());
    forbiddenServerManager.addToBlacklist(node.getNodeUrl().getIpAddress());
    Assert.assertEquals(nodeNum, leaseManager.getLeaseMeta().getClusterMembers().size());
  }

  @Test
  public void testFilterOut() {
    int nodeNum = 100;
    for (int i = 1; i < nodeNum; i++) {
      leaseManager.renew(new SimpleNode("10.0.0." + i), 100);
    }
    leaseManager.renew(new SimpleNode("127.0.0.1"), 100);
    List<Lease> leases =
        leaseManager.filterOut(
            Lists.newArrayList(leaseManager.localRepo.values()),
            new LeaseFilter<SimpleNode>() {
              @Override
              public boolean allowSelect(Lease<SimpleNode> lease) {
                return !lease.getRenewal().getNodeUrl().getIpAddress().startsWith("10.0.0");
              }
            });
    Assert.assertEquals(1, leases.size());
  }
}
