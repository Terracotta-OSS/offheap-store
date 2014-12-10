/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.offheapstore.storage.listener;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

/**
 *
 * @author cdennis
 */
public interface RecoveryStorageEngineListener<K, V> extends StorageEngineListener<K, V> {
  
  void recovered(Callable<? extends K> key, Callable<? extends V> value, ByteBuffer binaryKey, ByteBuffer binaryValue, int hash, int metadata, long encoding);
}
