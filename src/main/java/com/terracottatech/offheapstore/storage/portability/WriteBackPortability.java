package com.terracottatech.offheapstore.storage.portability;

import java.nio.ByteBuffer;

import com.terracottatech.offheapstore.storage.portability.Portability;

public interface WriteBackPortability<T> extends Portability<T> {

  T decode(ByteBuffer buffer, WriteContext context);
}
