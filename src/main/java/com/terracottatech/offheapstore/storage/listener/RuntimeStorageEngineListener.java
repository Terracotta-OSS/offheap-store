package com.terracottatech.offheapstore.storage.listener;

import java.nio.ByteBuffer;


public interface RuntimeStorageEngineListener<K, V> extends StorageEngineListener<K, V> {

  void written(K key, V value, ByteBuffer binaryKey, ByteBuffer binaryValue, int hash, int metadata, long encoding);
  
  void freed(long encoding, int hash, ByteBuffer binaryKey, boolean removed);

  void cleared();

  void copied(int hash, long oldEncoding, long newEncoding, int metadata);
}
