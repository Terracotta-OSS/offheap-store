/*
 * Copyright 2014-2023 Terracotta, Inc., a Software AG company.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
