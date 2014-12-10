package com.terracottatech.offheapstore.storage.listener;

public interface ListenableStorageEngine<K, V> {

  void registerListener(StorageEngineListener<? super K, ? super V> listener);
}
