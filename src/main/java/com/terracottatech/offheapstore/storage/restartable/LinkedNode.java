package com.terracottatech.offheapstore.storage.restartable;


public interface LinkedNode<T> {

  long getLsn();
  
  void setLsn(long lsn);
  
  long getNext();
  long getPrevious();
  
  void setNext(long encoding);
  void setPrevious(long encoding);
  
  T getValue();
  
  void flush();
  
  int getMetadata();
}
