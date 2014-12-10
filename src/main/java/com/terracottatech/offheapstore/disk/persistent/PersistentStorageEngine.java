/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.persistent;

import com.terracottatech.offheapstore.storage.StorageEngine;

/**
 *
 * @author Chris Dennis
 */
public interface PersistentStorageEngine<K, V> extends StorageEngine<K, V>, Persistent {

}
