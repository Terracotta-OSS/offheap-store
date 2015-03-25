////
Copyright 2015 Terracotta, Inc., a Software AG company.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
////

= OffHeap Store

OffHeap Store is a library that provides a set of map and cache implementations
that store data outside of the normal Java heap.  Additionally it provides a
bunch of interesting things to support these functions that might be interesting
in isolation to some people.

Licensed under the Apache License, Version 2.0   
(C) Terracotta, Inc., a Software AG company.

http://www.terracotta.org

See also: http://www.github.com/ehcache/ehcache3

Status of the build: image:https://terracotta-oss.ci.cloudbees.com/buildStatus/icon?job=offheap-store[Terracotta-OSS@Cloudbees, link="https://terracotta-oss.ci.cloudbees.com/job/offheap-store/"]

== What's Available
On the surface OffHeap Store contains implementations of:

 * Map (non thread-safe)
 * ConcurrentMap (single stripe or segmented; read-write locked or exclusively locked)
 * Set (but use +Collections.newSetFromMap(Map)+ instead)
 * Clock Cache (single stripe or segmented; read-write locked or exclusively locked)

Additional functionality includes:

 * Cache Entry Pinning
 * Eviction Listeners
 * _Non-fault tolerant_ disk backend

Things that might be interesting to some:

 * Per entry metadata (very basic API)
 * Serialization optimization (redundant ObjectStreamClass descriptor compression)
 * Native heap-alike implementation (+OffHeapStorageArea+)
 * A (crude) weak identity hash map (because the world needed one more)

== Structure

Like all software OffHeap Store is just a big stack of abstractions, rough structure starting at
the bottom and working up.

[horizontal]
  +BufferSource+::       +ByteBuffer+ factories [+org.terracotta.offheapstore.buffersource+]
  +PageSource+::         +Page+ factories, that uses ByteBuffers) [+org.terracotta.offheapstore.paging+]
  +OffHeapStorageArea+:: native heap-alike that uses pages)
  +StorageEngine+::      provide storage for POJOs, some use +OffHeapStorageArea+)
  +OffHeapHashMap+::     core map implementation, uses storage engine for K/V storage and a page for the hashtable.
  a million subclasses:: all the map derivatives: concurrent, evicting (caches), et al.

image:https://www.cloudbees.com/sites/default/files/styles/large/public/Button-Powered-by-CB.png?itok=uMDWINfY[Cloudbees, link="http://www.cloudbees.com/resources/foss"]

