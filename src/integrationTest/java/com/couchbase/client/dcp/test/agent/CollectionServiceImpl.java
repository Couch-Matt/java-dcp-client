/*
 * Copyright 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.test.agent;

import com.couchbase.client.core.error.CollectionExistsException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.ScopeExistsException;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.util.ConsistencyUtil;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class CollectionServiceImpl implements CollectionService {
  private static final Logger log = LoggerFactory.getLogger(CollectionServiceImpl.class);

  private final ClusterSupplier clusterSupplier;

  public CollectionServiceImpl(ClusterSupplier clusterSupplier) {
    this.clusterSupplier = requireNonNull(clusterSupplier);
  }

  private Cluster cluster() {
    return clusterSupplier.get();
  }

  private CollectionManager collectionManager(String bucket) {
    return cluster().bucket(bucket).collections();
  }

  @Override
  public List<String> createScopesWithPrefix(String bucket, String scopeIdPrefix, int scopes) {
    List<String> scopeIds = generateIds(scopeIdPrefix, scopes);
    createScopes(scopeIds, bucket);
    log.info("{} Scopes created, with prefix {}", scopes, scopeIdPrefix);
    return scopeIds;
  }

  @Override
  public List<String> createCollectionsWithPrefix(String bucket, String scope, String collectionIdPrefix, int collections) {
    List<String> collectionIds = generateIds(collectionIdPrefix, collections);
    createCollections(collectionIds, scope, bucket);
    log.info("{} Collections created, with prefix {}, on scope {}", collections, collectionIdPrefix, scope);
    return collectionIds;
  }

  @Override
  public void createScopes(List<String> scopes, String bucket) {
    CollectionManager collectionManager = collectionManager(bucket);
    scopes.forEach(scopeName ->
        retryOnChannelClosed(() -> collectionManager.createScope(scopeName), ScopeExistsException.class)
    );
    scopes.forEach(scopeName ->
        ConsistencyUtil.waitUntilScopePresent(clusterSupplier.get().core(), bucket, scopeName)
    );
  }

  @Override
  public void createCollections(List<String> collections, String scope, String bucket) {
    CollectionManager collectionManager = collectionManager(bucket);
    collections.forEach(collectionName -> {
      CollectionSpec collectionSpec = CollectionSpec.create(collectionName, scope);
      retryOnChannelClosed(() -> collectionManager.createCollection(collectionSpec), CollectionExistsException.class);
    });

    collections.forEach(collectionName ->
        ConsistencyUtil.waitUntilCollectionPresent(clusterSupplier.get().core(), bucket, scope, collectionName)
    );
  }

  @Override
  public void deleteScopes(List<String> scopes, String bucket) {
    CollectionManager collectionManager = collectionManager(bucket);
    scopes.forEach(collectionManager::dropScope);
    scopes.forEach(scopeName ->
        ConsistencyUtil.waitUntilScopeDropped(clusterSupplier.get().core(), bucket, scopeName)
    );

  }

  @Override
  public void deleteCollections(List<String> collections, String scope, String bucket) {
    CollectionManager collectionManager = collectionManager(bucket);
    collections.forEach(collectionID -> collectionManager.dropCollection(CollectionSpec.create(collectionID, scope)));
    collections.forEach(collectionName ->
        ConsistencyUtil.waitUntilCollectionDropped(clusterSupplier.get().core(), bucket, scope, collectionName)
    );
  }

  /**
   * Runs a non-idempotent management request, retrying if the HTTP connection is closed mid-request.
   * The SDK does not retry these itself, and in CI the management connection is occasionally dropped
   * just after a bucket is created. If the first attempt actually reached the server, a retry fails
   * with {@code alreadyDone}, which is treated as success.
   */
  private static void retryOnChannelClosed(Runnable op, Class<? extends RuntimeException> alreadyDone) {
    final int maxAttempts = 3;
    for (int attempt = 1; ; attempt++) {
      try {
        op.run();
        return;
      } catch (RequestCanceledException e) {
        boolean channelClosed = e.reason().innerReason() == RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT;
        if (!channelClosed || attempt >= maxAttempts) {
          throw e;
        }
        log.warn("Management request failed because the channel closed while in flight; retrying (attempt {} of {})",
            attempt + 1, maxAttempts, e);
      } catch (RuntimeException e) {
        if (attempt > 1 && alreadyDone.isInstance(e)) {
          log.info("Management request succeeded on an earlier attempt: {}", e.toString());
          return;
        }
        throw e;
      }
    }
  }

  private static List<String> generateIds(String prefix, int numberToGenerate) {
    return IntStream.range(0, numberToGenerate)
        .mapToObj(i -> prefix + i)
        .collect(toList());
  }
}
