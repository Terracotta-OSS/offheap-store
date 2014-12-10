package com.terracottatech.offheapstore.storage.restartable;


public class DetachedLinkedNode<T> implements LinkedNode<T> {

  private final T value;
  
  public DetachedLinkedNode(T value) {
    this.value = value;
  }
  
  @Override
  public long getLsn() {
    return -1L;
  }

  @Override
  public void setLsn(long lsn) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getNext() {
    return RestartableStorageEngine.NULL_ENCODING;
  }

  @Override
  public long getPrevious() {
    return RestartableStorageEngine.NULL_ENCODING;
  }

  @Override
  public void setNext(long encoding) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setPrevious(long encoding) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void flush() {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public T getValue() {
    return value;
  }
  
  @Override
  public int getMetadata() {
    throw new UnsupportedOperationException();
  }
}
