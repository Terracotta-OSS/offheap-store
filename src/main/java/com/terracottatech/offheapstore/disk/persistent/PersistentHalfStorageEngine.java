/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.persistent;

import com.terracottatech.offheapstore.storage.HalfStorageEngine;

/**
 *
 * @author cdennis
 */
public interface PersistentHalfStorageEngine<T> extends HalfStorageEngine<T>, Persistent {

}
