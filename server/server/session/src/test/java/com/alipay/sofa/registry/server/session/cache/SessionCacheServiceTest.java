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
package com.alipay.sofa.registry.server.session.cache;

import com.alipay.sofa.registry.common.model.store.SubDatum;
import com.alipay.sofa.registry.server.session.TestUtils;
import com.alipay.sofa.registry.server.session.node.service.DataNodeService;
import java.util.Collections;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class SessionCacheServiceTest {
  private final String dataInfoId = "testDataInfoId";
  private final String dataCenter = "testDc";

  @Test
  public void testGet() {
    SessionCacheService cacheService = new SessionCacheService();
    cacheService.sessionServerConfig = TestUtils.newSessionConfig(dataCenter);

    DatumCacheGenerator generator = new DatumCacheGenerator();
    cacheService.setCacheGenerators(Collections.singletonMap(DatumKey.class.getName(), generator));
    generator.dataNodeService = Mockito.mock(DataNodeService.class);

    cacheService.init();
    DatumKey datumKey = new DatumKey(dataInfoId, dataCenter);
    Key key = new Key(DatumKey.class.getName(), datumKey);
    Assert.assertEquals(key, new Key(DatumKey.class.getName(), datumKey));
    Assert.assertTrue(key.toString(), key.toString().contains(dataInfoId));
    Assert.assertEquals(key.getEntityName(), DatumKey.class.getName());
    Assert.assertEquals(key.getEntityType(), datumKey);
    Assert.assertEquals(key.getEntityType().hashCode(), datumKey.hashCode());

    Value value = cacheService.getValueIfPresent(key);
    Assert.assertNull(value);

    value = cacheService.getValue(key);
    Assert.assertNull(value.getPayload());

    SubDatum subDatum =
        SubDatum.normalOf(
            dataInfoId,
            dataCenter,
            100,
            Collections.emptyList(),
            "testDataId",
            "testInstanceId",
            "testGroup",
            Lists.newArrayList(System.currentTimeMillis()));

    Mockito.when(generator.dataNodeService.fetch(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(subDatum);
    // invalidate the null value
    cacheService.invalidate(key);
    value = cacheService.getValue(key);
    Assert.assertEquals(value.getPayload(), subDatum);
    value = cacheService.getValueIfPresent(key);
    Assert.assertEquals(value.getPayload(), subDatum);
    cacheService.invalidate(key);

    value = cacheService.getValueIfPresent(key);
    Assert.assertNull(value);

    // touch remove listener
    for (int i = 0; i < 1000; i++) {
      datumKey = new DatumKey(dataInfoId + ":" + i, dataCenter);
      key = new Key(DatumKey.class.getName(), datumKey);
      cacheService.getValue(key);
    }
  }
}
