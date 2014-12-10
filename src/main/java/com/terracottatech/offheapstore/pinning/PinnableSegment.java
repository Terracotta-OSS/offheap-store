package com.terracottatech.offheapstore.pinning;

import com.terracottatech.offheapstore.Segment;

public interface PinnableSegment<K, V> extends Segment<K, V>, PinnableCache<K, V> {

}
