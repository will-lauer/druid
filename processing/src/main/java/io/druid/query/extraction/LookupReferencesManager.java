/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.extraction;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.guice.ManageLifecycle;

import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class provide a basic {@link LookupExtractorFactory} references manager.
 * It allows basic operations fetching, listing, adding and deleting of {@link LookupExtractor} objects
 * It is be used by queries to fetch the lookup reference.
 * It is used by Lookup configuration manager to add/remove or list lookups configuration via HTTP or other protocols.
 */

@ManageLifecycle
public class LookupReferencesManager
{
  private static final Logger LOGGER = new Logger(LookupReferencesManager.class);
  private final ConcurrentMap<String, LookupExtractorFactory> lookupMap = new ConcurrentHashMap();
  private final Object lock = new Object();
  private final AtomicBoolean started = new AtomicBoolean(false);

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (!started.getAndSet(true)) {
        LOGGER.info("Started lookup factory references manager");
      }
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (started.getAndSet(false)) {
        LOGGER.info("Stopped lookup factory references manager");
        for (String lookupName : lookupMap.keySet()) {
          remove(lookupName);
        }
      }
    }
  }

  /**
   * @param lookupName             namespace of the lookupExtractorFactory object
   * @param lookupExtractorFactory {@link LookupExtractorFactory} implementation reference.
   *
   * @return true if the lookup is added otherwise false.
   *
   * @throws ISE If the manager is closed or if start of lookup returns false.
   */
  public boolean put(String lookupName, final LookupExtractorFactory lookupExtractorFactory) throws ISE
  {
    synchronized (lock) {
      assertStarted();
      if (!lookupExtractorFactory.start()) {
        throw new ISE("start method returned false for lookup [%s]", lookupName);
      }
      return (null == lookupMap.putIfAbsent(lookupName, lookupExtractorFactory));
    }
  }

  /**
   * @param lookups {@link ImmutableMap<String, LookupExtractorFactory>} containing all the lookup as one batch.
   *
   * @throws ISE if the manager is closed or if {@link LookupExtractorFactory#start()} returns false
   */
  public void put(ImmutableMap<String, LookupExtractorFactory> lookups) throws ISE
  {
    synchronized (lock) {
      assertStarted();
      for (ImmutableMap.Entry<String, LookupExtractorFactory> entry : lookups.entrySet()) {
        final String lookupName = entry.getKey();
        final LookupExtractorFactory lookupExtractorFactory = entry.getValue();
        if (!lookupExtractorFactory.start()) {
          throw new ISE("start method returned false for lookup [%s]", lookupName);
        }
        if (null != lookupMap.putIfAbsent(lookupName, lookupExtractorFactory)) {
          LOGGER.warn("lookup with name [%s] is not add since it already exist", lookupName);
        }
      }
    }
  }

  /**
   * @param lookupName namespace of {@link LookupExtractor} to delete from the reference registry.
   *                   this function does call the cleaning method {@link LookupExtractor#close()}
   *
   * @return true only if {@code lookupName} is removed and the lookup correctly stopped
   */
  public boolean remove(String lookupName)
  {
    final LookupExtractorFactory lookupExtractorFactory = lookupMap.remove(lookupName);
    if (lookupExtractorFactory != null) {
      LOGGER.debug("Removing lookup [%s]", lookupName);
      return lookupExtractorFactory.stop();
    }
    return false;
  }

  /**
   * @param lookupName namespace key to fetch the reference of the object {@link LookupExtractor}
   *
   * @return reference of {@link LookupExtractor} that correspond the the namespace {@code lookupName} or null if absent
   *
   * @throws ISE if the {@link LookupReferencesManager} is closed or did not start yet
   */
  @Nullable
  public LookupExtractorFactory get(String lookupName) throws ISE
  {
    final LookupExtractorFactory lookupExtractorFactory = lookupMap.get(lookupName);
    assertStarted();
    return lookupExtractorFactory;
  }

  /**
   * @return Returns {@link ImmutableMap} containing a copy of the current state.
   *
   * @throws ISE if the is is closed or did not start yet.
   */
  public ImmutableMap<String, LookupExtractorFactory> getAll() throws ISE
  {
    assertStarted();
    return ImmutableMap.copyOf(lookupMap);
  }

  private void assertStarted() throws ISE
  {
    if (isClosed()) {
      throw new ISE("lookup manager is closed");
    }
  }

  @VisibleForTesting
  protected boolean isClosed()
  {
    return !started.get();
  }
}
