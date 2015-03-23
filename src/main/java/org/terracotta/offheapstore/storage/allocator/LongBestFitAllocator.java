/* 
 * Copyright 2015 Terracotta, Inc., a Software AG company.
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

/*
 * This file contains a Java port of Doug Lea's malloc implementation
 * (ftp://g.oswego.edu/pub/misc/malloc.c), which has been released to the public
 * domain as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.terracotta.offheapstore.storage.allocator;

import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.util.FindbugsSuppressWarnings;

import java.util.Iterator;
import java.util.NoSuchElementException;
import static org.terracotta.offheapstore.util.Validation.shouldValidate;
import static org.terracotta.offheapstore.util.Validation.validate;

/**
 * An aggressively best-fit allocator.
 * 
 * @author Chris Dennis
 */
public final class LongBestFitAllocator implements Allocator {

  private static final boolean DEBUG = Boolean.getBoolean(LongBestFitAllocator.class.getName() + ".DEBUG");
  private static final boolean VALIDATING = shouldValidate(LongBestFitAllocator.class);

  /* The byte and bit size of a size_t */
  private static final int SIZE_T_BITSIZE    = Long.SIZE;
  private static final int SIZE_T_SIZE       = SIZE_T_BITSIZE / Byte.SIZE;

  private static final int MALLOC_ALIGNMENT = 2 * SIZE_T_SIZE;
  
  /* The bit mask value corresponding to MALLOC_ALIGNMENT */
  private static final int CHUNK_ALIGN_MASK  = MALLOC_ALIGNMENT - 1;
  
  /* ------------------- Chunks sizes and alignments ----------------------- */
  private static final int MCHUNK_SIZE       = 4 * SIZE_T_SIZE;

  private static final int CHUNK_OVERHEAD    = 2 * SIZE_T_SIZE;

  /* The smallest size we can malloc is an aligned minimal chunk */
  private static final long MIN_CHUNK_SIZE    = (MCHUNK_SIZE + CHUNK_ALIGN_MASK) & ~CHUNK_ALIGN_MASK;

  /* Bounds on request (not chunk) sizes. */
  private static final long MAX_REQUEST       = ((-MIN_CHUNK_SIZE) << 2) & Integer.MAX_VALUE;
  private static final long MIN_REQUEST       = MIN_CHUNK_SIZE - CHUNK_OVERHEAD - 1;

  private static final long TOP_FOOT_SIZE     = alignOffset(chunkToMem(0)) + padRequest(MIN_CHUNK_SIZE);
  
  private static final long MINIMAL_SIZE       = Long.highestOneBit(TOP_FOOT_SIZE) << 1;
  
  private static final long TOP_FOOT_OFFSET    = memToChunk(0) + TOP_FOOT_SIZE;
  
  /* ------------------ Operations on head and foot fields ----------------- */

  /*
   * The head field of a chunk is or'ed with PINUSE_BIT when previous
   * adjacent chunk in use, and or'ed with CINUSE_BIT if this chunk is in
   * use.
   *
   * FLAG4_BIT is not used by this malloc, but might be useful in extensions.
   */

  private static final int PINUSE_BIT        = 1;
  private static final int CINUSE_BIT        = 2;
  private static final int FLAG4_BIT         = 4;
  private static final int INUSE_BITS        = PINUSE_BIT | CINUSE_BIT;
  private static final int FLAG_BITS         = PINUSE_BIT | CINUSE_BIT | FLAG4_BIT;

  /* Bin types, widths and sizes */
  private static final int NSMALLBINS        = 32;
  private static final int NTREEBINS         = 32;
  private static final int SMALLBIN_SHIFT    = 3;
  private static final int TREEBIN_SHIFT     = 8;
  private static final int MIN_LARGE_SIZE    = 1 << TREEBIN_SHIFT;
  private static final int MAX_SMALL_SIZE    = MIN_LARGE_SIZE - 1;
  private static final int MAX_SMALL_REQUEST = MAX_SMALL_SIZE - CHUNK_ALIGN_MASK - CHUNK_OVERHEAD;

  private final OffHeapStorageArea storage;

  private int smallMap;
  private int treeMap;
  
  private final long[] smallBins = new long[NSMALLBINS];
  private final long[] treeBins = new long[NTREEBINS];
  
  private long designatedVictimSize = 0;
  private long designatedVictim = -1;
  
  private long topSize = 0;
  private long top = 0;
  
  private long occupied;
  
  /**
   * Create a best fit allocator backed by the given OffHeapStorageArea.
   *
   * @param storage source of ByteBuffer instances
   */
  public LongBestFitAllocator(OffHeapStorageArea storage) {
    this.storage = storage;
    clear();
  }

  @Override
  public void clear() {
    //initialize state
    top = 0;
    topSize = -TOP_FOOT_SIZE;
    //head(top, topSize | PINUSE_BIT);
    designatedVictim = -1;
    designatedVictimSize = 0;
    for (int i = 0; i < treeBins.length; i++) {
      treeBins[i] = -1;
      clearTreeMap(i);
    }
    for (int i = 0; i < smallBins.length; i++) {
      smallBins[i] = -1;
      clearSmallMap(i);
    }
    occupied = 0;
  }

  @Override
  public void expand(long increase) {
    //update allocator state for increased size
    topSize += increase;
    head(top, topSize | PINUSE_BIT);
    if (topSize >= 0) checkTopChunk(top);
  }

  @Override
  public long allocate(long size) {
    return dlmalloc(size);
  }

  @Override
  public void free(long address) {
    dlfree(address, true);
  }

  @Override
  public long occupied() {
    return occupied;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    if (DEBUG) {
      sb.append("\nChunks:").append(dump());
    }
    return sb.toString();
  }

  private String dump() {
    StringBuilder sb = new StringBuilder();

    long q = 0;
    while (q != top) {
      if (isInUse(q)) {
        sb.append(" InUseChunk:&").append(q).append(':').append(chunkSize(q)).append('b');
      } else {
        sb.append(" FreeChunk:&").append(q).append(':').append(chunkSize(q)).append('b');
      }
      q = nextChunk(q);
    }
    sb.append(" TopChunk:&").append(top).append(':').append(topSize + TOP_FOOT_SIZE).append('b');
    return sb.toString();
  }

  private long dlmalloc(long bytes) {
    /*
     * Basic algorithm:
     * If a small request (< 256 bytes minus per-chunk overhead):
     *   1. If one exists, use a remainderless chunk in associated smallbin.
     *      (Remainderless means that there are too few excess bytes to
     *      represent as a chunk.)
     *   2. If it is big enough, use the dv chunk, which is normally the
     *      chunk adjacent to the one used for the most recent small request.
     *   3. If one exists, split the smallest available chunk in a bin,
     *      saving remainder in dv.
     *   4. If it is big enough, use the top chunk.
     *   5. If available, get memory from system and use it
     * Otherwise, for a large request:
     *   1. Find the smallest available binned chunk that fits, and use it
     *      if it is better fitting than dv chunk, splitting if necessary.
     *   2. If better fitting than any binned chunk, use the dv chunk.
     *   3. If it is big enough, use the top chunk.
     *   4. If request size >= mmap threshold, try to directly mmap this chunk.
     *   5. If available, get memory from system and use it
     * 
     */
    
    long nb = (bytes < MIN_REQUEST) ? MIN_CHUNK_SIZE : padRequest(bytes); //internal request size;
    
    if (bytes <= MAX_SMALL_REQUEST) {      
      int index = smallBinIndex(nb);
      
      //bit map of all occupied bins big enough for this request
      int smallBits = smallMap >>> index;
    
      if ((smallBits & 0x3) != 0) {
        //remainderless fit to a small bin is possible
        index += ~smallBits & 1; //use next bin if ours is empty        

        return allocateFromSmallBin(index, nb);
      } else if (nb > designatedVictimSize) {
        if (smallBits != 0) { //some small bin is big enough for us
          return splitFromSmallBin(Integer.numberOfTrailingZeros(smallBits << index), nb);
        } else if (treeMap != 0) {
          return splitSmallFromTree(nb);
        }
      }
    } else if (bytes > MAX_REQUEST) {
      return -1L;
    } else if (treeMap != 0) {
      long mem = splitFromTree(nb);
      if (okAddress(mem)) {
        return mem;
      }
    }
    
    if (nb <= designatedVictimSize) {
      return splitFromDesignatedVictim(nb);
    } else if (nb < topSize) {
      return splitFromTop(nb);
    }
    
    return -1L;
  }

  private long allocateFromSmallBin(int index, long nb) {
    long h = smallBins[index];
    validate(!VALIDATING || chunkSize(h) == smallBinIndexToSize(index));
    
    long f = forward(h);
    long b = backward(h);
    
    if (f == h) {
      validate(!VALIDATING || b == h);
      clearSmallMap(index);
      smallBins[index] = -1;
    } else {
      smallBins[index] = f;
      backward(f, b);
      forward(b, f);
    }
    setInUseAndPreviousInUse(h, smallBinIndexToSize(index));
    long mem = chunkToMem(h);
    checkMallocedChunk(mem, nb);
    return mem;
  }

  private long splitFromSmallBin(int index, long nb) {
    long h = smallBins[index];
    validate(!VALIDATING || chunkSize(h) == smallBinIndexToSize(index));
    
    long f = forward(h);
    long b = backward(h);
    
    if (f == h) {
      validate(!VALIDATING || b == h);
      clearSmallMap(index);
      smallBins[index] = -1;
    } else {
      smallBins[index] = f;
      backward(f, b);
      forward(b, f);
    }
    
    long rsize = smallBinIndexToSize(index) - nb;
    
    /* Fit here cannot be remainderless if 4byte sizes */
    if (rsize < MIN_CHUNK_SIZE) {
      setInUseAndPreviousInUse(h, smallBinIndexToSize(index));
    } else {
      setSizeAndPreviousInUseOfInUseChunk(h, nb);
      long r = h + nb;
      setSizeAndPreviousInUseOfFreeChunk(r, rsize);
      replaceDesignatedVictim(r, rsize);
    }
    
    long mem = chunkToMem(h);
    checkMallocedChunk(mem, nb);
    return mem;
  }
  
  private void replaceDesignatedVictim(long p, long s) {
    long dvs = designatedVictimSize;
    if (dvs != 0) {
      long dv = designatedVictim;
      validate(!VALIDATING || isSmall(dvs));
      insertSmallChunk(dv, dvs);
    }
    designatedVictimSize = s;
    designatedVictim = p;
  }

  private long splitSmallFromTree(long nb) {
    int index = Integer.numberOfTrailingZeros(treeMap);
    
    long t;
    long v = t = treeBins[index];
    long rsize = chunkSize(t) - nb;

    while ((t = leftmostChild(t)) != -1L) {
      long trem = chunkSize(t) - nb;
      if (trem >= 0 && trem < rsize) {
        rsize = trem;
        v = t;
      }
    }

    if (okAddress(v)) {
      long r = v + nb;
      validate(!VALIDATING || chunkSize(v) == rsize + nb);
      if (okNext(v, r)) {
        unlinkLargeChunk(v);
        if (rsize < MIN_CHUNK_SIZE) {
          setInUseAndPreviousInUse(v, rsize + nb);
        } else {
          setSizeAndPreviousInUseOfInUseChunk(v, nb);
          setSizeAndPreviousInUseOfFreeChunk(r, rsize);
          replaceDesignatedVictim(r, rsize);
        }
        long mem = chunkToMem(v);
        checkMallocedChunk(mem, nb);
        return mem;
      } else {
        throw new AssertionError();
      }
    } else {
      throw new AssertionError();
    }
  }

  private long splitFromTree(long nb) {
    long v = -1;
    long rsize = Long.MAX_VALUE & (-nb); /* Unsigned negation */
    long t;
    
    int index = treeBinIndex(nb);
    
    if ((t = treeBins[index]) != -1) {
      /* Traverse tree for this bin looking for node with size == nb */
      long sizebits = nb << leftShiftForTreeIndex(index);
      long rst = -1L;  /* The deepest untaken right subtree */
      for (;;) {
        long rt;
        long trem = chunkSize(t) - nb;
        if (trem >= 0 && trem < rsize) {
          v = t;
          if ((rsize = trem) == 0)
            break;
        }
        rt = child(t, 1);
        t = child(t, (int) (sizebits >>> (SIZE_T_BITSIZE - 1)));
        if (rt != -1 && rt != t)
          rst = rt;
        if (t == -1) {
          t = rst; /* set t to least subtree holding sizes > nb */
          break;
        }
        sizebits <<= 1;
      }
    }
    if (t == -1 && v == -1) { /* set t to root of next non-empty treebin */
      int leftbits = leftBits(1 << index) & treeMap;
      if (leftbits != 0) {
        t = treeBins[Integer.numberOfTrailingZeros(leftbits)];
      }
    }

    while (t != -1) { /* find smallest of tree or subtree */
      long trem = chunkSize(t) - nb;
      if (trem >= 0 && trem < rsize) {
        rsize = trem;
        v = t;
      }
      t = leftmostChild(t);
    }

    /*  If dv is a better fit, return 0 so malloc will use it */
    long designatedVictimFit = designatedVictimSize - nb;
    if (v != -1 && (designatedVictimFit < 0 || rsize < designatedVictimFit)) {
      if (okAddress(v)) { /* split */
        long r = v + nb;
        validate(!VALIDATING || chunkSize(v) == rsize + nb);
        if (okNext(v, r)) {
          unlinkLargeChunk(v);
          if (rsize < MIN_CHUNK_SIZE) {
            setInUseAndPreviousInUse(v, rsize + nb);
          } else {
            setSizeAndPreviousInUseOfInUseChunk(v, nb);
            setSizeAndPreviousInUseOfFreeChunk(r, rsize);
            insertChunk(r, rsize);
          }
          return chunkToMem(v);
        }
      } else {
        throw new AssertionError();
      }
    }
    return -1L;
  }

  private long splitFromDesignatedVictim(long nb) {
    long rsize = designatedVictimSize - nb;
    long p = designatedVictim;
    
    if (rsize >= MIN_CHUNK_SIZE) { /* split dv */
      long r = designatedVictim = p + nb;
      designatedVictimSize = rsize;
      setSizeAndPreviousInUseOfFreeChunk(r, rsize);
      setSizeAndPreviousInUseOfInUseChunk(p, nb);
    } else { /* exhaust dv */
      long dvs = designatedVictimSize;
      designatedVictimSize = 0;
      designatedVictim = -1;
      setInUseAndPreviousInUse(p, dvs);
    }
    long mem = chunkToMem(p);
    checkMallocedChunk(mem, nb);
    return mem;
  }

  private long splitFromTop(long nb) {
    long rSize = topSize -= nb;
    long p = top;
    long r = top = p + nb;
    head(r, rSize | PINUSE_BIT);
    setSizeAndPreviousInUseOfInUseChunk(p, nb);
    long mem = chunkToMem(p);
    checkTopChunk(top);
    checkMallocedChunk(mem, nb);
    return mem;
  }

  private void dlfree(long mem, boolean shrink) {
    /*
     * Consolidate freed chunks with preceeding or succeeding bordering free
     * chunks, if they exist, and then place in a bin. Intermixed with special
     * cases for top, designatedVictim, and usage errors.
     */
    long p = memToChunk(mem);

    if (okAddress(p) && isInUse(p)) {
      checkInUseChunk(p);
      long psize = chunkSize(p); 
      occupied -= psize;
      long next = p + psize;
      
      if (!previousInUse(p)) {
        long previousSize = prevFoot(p);
        
        long previous = p - previousSize;
        psize += previousSize;
        p = previous;
        if (okAddress(previous)) {
          if (p != designatedVictim) {
            unlinkChunk(p, previousSize);
          } else if ((head(next) & INUSE_BITS) == INUSE_BITS) {
            designatedVictimSize = psize;
            setFreeWithPreviousInUse(p, psize, next);
            return;
          }
        } else {
          throw new AssertionError();
        }
      }
      
      if (okNext(p, next) && previousInUse(next)) {
        if (!chunkInUse(next)) { // consolidate forward
          if (next == top) {
            long tsize = topSize += psize;
            top = p;
            head(p, tsize | PINUSE_BIT);
            if (p == designatedVictim) {
              designatedVictim = -1;
              designatedVictimSize = 0;
            }
            if (shrink) {
              storage.release(p + TOP_FOOT_SIZE);
            }
            return;
          } else if (next == designatedVictim) {
            long dsize = designatedVictimSize += psize;
            designatedVictim = p;
            setSizeAndPreviousInUseOfFreeChunk(p, dsize);
            return;
          } else {
            long nsize = chunkSize(next);
            psize += nsize;
            unlinkChunk(next, nsize);
            setSizeAndPreviousInUseOfFreeChunk(p, psize);
            if (p == designatedVictim) {
              designatedVictimSize = psize;
              return;
            }
          }
        } else {
          setFreeWithPreviousInUse(p, psize, next);
        }
        
        if (isSmall(psize)) {
          insertSmallChunk(p, psize);
        } else {
          insertLargeChunk(p, psize);
        }
      } else {
        throw new AssertionError("Problem with next chunk [" + p + "][" + next + ":previous-inuse=" + previousInUse(next) + "]");
      }
    } else {
      throw new IllegalArgumentException("Address " + mem + " has not been allocated");
    }
  }

  private void insertChunk(long p, long s) {
    if (isSmall(s)) {
      insertSmallChunk(p, s);
    } else {
      insertLargeChunk(p, s);
    }
  }
  
  private void insertSmallChunk(long p, long s) {
    int index = smallBinIndex(s);

    long h = smallBins[index];

    if (!smallMapIsMarked(index)) {
      markSmallMap(index);
      smallBins[index] = p;
      forward(p, p);
      backward(p, p);
    } else if (okAddress(h)) {
      long b = backward(h);
      forward(b, p);
      forward(p, h);
      backward(h, p);
      backward(p, b);
    } else {
      throw new AssertionError();
    }
    checkFreeChunk(p);
  }

  private void insertLargeChunk(long x, long s) {
    int index = treeBinIndex(s);
    long h = treeBins[index];

    
    index(x, index);
    child(x, 0, -1);
    child(x, 1, -1);

    if (!treeMapIsMarked(index)) {
      markTreeMap(index);
      treeBins[index] = x;
      parent(x, -1);
      forward(x, x);
      backward(x, x);
    } else {
      long t = h;
      long k = s << leftShiftForTreeIndex(index);
      for (;;) {
        if (chunkSize(t) != s) {
          int childIndex = (int) ((k >>> (SIZE_T_BITSIZE - 1)) & 1);          
          long child = child(t, childIndex);
          k <<= 1;
          if (okAddress(child)) { 
            t = child;
          } else {
            child(t, childIndex, x);
            parent(x, t);
            forward(x, x);
            backward(x, x);
            break;
          }
        } else {
          long f = forward(t);
          if (okAddress(t) && okAddress(f)) {
            backward(f, x);
            forward(t, x);
            forward(x, f);
            backward(x, t);
            parent(x, -1);
            break;
          } else {
            throw new AssertionError();
          }
        }
      }
    }
    checkFreeChunk(x);
  }

  private void unlinkChunk(long p, long s) {
    if (isSmall(s)) {
      unlinkSmallChunk(p, s);
    } else {
      unlinkLargeChunk(p);
    }
  }

  private void unlinkSmallChunk(long p, long s) {
    long f = forward(p);
    long b = backward(p);
    
    int index = smallBinIndex(s);

    validate(!VALIDATING || chunkSize(p) == smallBinIndexToSize(index));
    
    if (f == p) {
      validate(!VALIDATING || b == p);
      clearSmallMap(index);
      smallBins[index] = -1;
    } else if (okAddress(smallBins[index])) {
      if (smallBins[index] == p) {
        smallBins[index] = f;
      }
      forward(b, f);
      backward(f, b);
    } else {
      throw new AssertionError();
    }
  }

  /*
   * Unlink steps:
   * 
   * 1. If x is a chained node, unlink it from its same-sized fd/bk links
   *    and choose its bk node as its replacement.
   * 2. If x was the last node of its size, but not a leaf node, it must
   *    be replaced with a leaf node (not merely one with an open left or
   *    right), to make sure that lefts and rights of descendents
   *    correspond properly to bit masks.  We use the rightmost descendent
   *    of x.  We could use any other leaf, but this is easy to locate and
   *    tends to counteract removal of leftmosts elsewhere, and so keeps
   *    paths shorter than minimally guaranteed.  This doesn't loop much
   *    because on average a node in a tree is near the bottom.
   * 3. If x is the base of a chain (i.e., has parent links) relink
   *    x's parent and children to x's replacement (or null if none).
   */
  private void unlinkLargeChunk(long x) {
    long xp = parent(x);
    long r;
    if (backward(x) != x) {
      long f = forward(x);
      r = backward(x);
      if (okAddress(f)) {
        backward(f, r);
        forward(r, f);
      } else {
        throw new AssertionError();
      }
    } else {
      int rpIndex;
      if (((r = child(x, rpIndex = 1)) != -1L) ||
          ((r = child(x, rpIndex = 0)) != -1L)) {
        long rp = x;
        while (true) {
          if (child(r, 1) != -1L) {
            rp = r;
            rpIndex = 1;
            r = child(r, 1);
          } else if (child(r, 0) != -1L) {
            rp = r;
            rpIndex = 0;
            r = child(r, 0);
          } else {
            break;
          }
        }
        
        if (okAddress(rp)) {
          child(rp, rpIndex, -1);
        } else {
          throw new AssertionError();
        }
      }
    }
    
    int index = index(x);
    if (xp != -1 || treeBins[(int) index] == x) {
      long h = treeBins[index];
      if (x == h) {
        if ((treeBins[index] = r) == -1) {
          clearTreeMap(index);
        } else {
          parent(r, -1);
        }
      } else if (okAddress(xp)) {
        if (child(xp, 0) == x) {
          child(xp, 0, r);
        } else {
          child(xp, 1, r);
        }
      } else {
        throw new AssertionError();
      }
      
      if (r != -1) {
        if (okAddress(r)) {
          long c0, c1;
          parent(r, xp);
          if ((c0 = child(x, 0)) != -1L) {
            if (okAddress(c0)) {
              child(r, 0, c0);
              parent(c0, r);
            } else {
              throw new AssertionError();
            }
          }
          if ((c1 = child(x, 1)) != -1L) {
            if (okAddress(c1)) {
              child(r, 1, c1);
              parent(c1, r);
            } else {
              throw new AssertionError();
            }
          }
        } else {
          throw new AssertionError();
        }
      }
    }
  }

  /* extraction of fields from head words */
  private boolean chunkInUse(long p) {
    return (head(p) & CINUSE_BIT) != 0;
  }
  
  private boolean previousInUse(long p) {
    return (head(p) & PINUSE_BIT) != 0;
  }

  private boolean isInUse(long p) {
    return (head(p) & INUSE_BITS) != PINUSE_BIT;
  }

  private long chunkSize(long p) {
    return head(p) & ~FLAG_BITS;
  }

  private void clearPreviousInUse(long p) {
    head(p, head(p) & ~PINUSE_BIT);
  }

  /* Ptr to next or previous physical malloc_chunk. */
  private long nextChunk(long p) {
    return p + chunkSize(p);
  }
  
  private long prevChunk(long p) {
    return p - prevFoot(p);
  }

  /* extract next chunk's pinuse bit */
  private boolean nextPreviousInUse(long p) {
    return (head(nextChunk(p)) & PINUSE_BIT) != 0;
  }

  /* Set size at footer */
  private void setFoot(long p, long s) {
    prevFoot(p + s, s);
  }

  /* Set size, pinuse bit, and foot */
  private void setSizeAndPreviousInUseOfFreeChunk(long p, long s) {
    head(p, s | PINUSE_BIT);
    setFoot(p, s);
  }

  /* Set size, pinuse bit, foot, and clear next pinuse */
  private void setFreeWithPreviousInUse(long p, long s, long n) {
    clearPreviousInUse(n);
    setSizeAndPreviousInUseOfFreeChunk(p, s);
  }

  /* Set cinuse and pinuse of this chunk and pinuse of next chunk */
  private void setInUseAndPreviousInUse(long p, long s) {
    setSizeAndPreviousInUseOfInUseChunk(p, s);
    head(p + s, head(p + s) | PINUSE_BIT);
  }
  
  /* Set size, cinuse and pinuse bit of this chunk */
  private void setSizeAndPreviousInUseOfInUseChunk(long p, long s) {
    head(p, s | PINUSE_BIT | CINUSE_BIT);
    setFoot(p, s);
    occupied += s;
  }
  
  private long prevFoot(long p) {
    return storage.readLong(p);
  }
  
  private void prevFoot(long p, long value) {
    storage.writeLong(p, value);
  }
  
  private long head(long p) {
    return storage.readLong(p + SIZE_T_SIZE);
  }

  private void head(long p, long value) {
    storage.writeLong(p + SIZE_T_SIZE, value);
  }
  
  private long forward(long p) {
    return storage.readLong(p + (2 * SIZE_T_SIZE));
  }
  
  private void forward(long p, long value) {
    storage.writeLong(p + (2 * SIZE_T_SIZE), value);
  }
  
  private long backward(long p) {
    return storage.readLong(p + (3 * SIZE_T_SIZE));
  }
  
  private void backward(long p, long value) {
    storage.writeLong(p + (3 * SIZE_T_SIZE), value);
  }
  
  @FindbugsSuppressWarnings("ICAST_INTEGER_MULTIPLY_CAST_TO_LONG")
  private long child(long p, int index) {
    return storage.readLong(p + ((4 + index) * SIZE_T_SIZE));
  }
  
  @FindbugsSuppressWarnings("ICAST_INTEGER_MULTIPLY_CAST_TO_LONG")
  private void child(long p, int index, long value) {
    storage.writeLong(p + ((4 + index) * SIZE_T_SIZE), value);
  }
  
  private long parent(long p) {
    return storage.readLong(p + (6 * SIZE_T_SIZE));
  }
  
  private void parent(long p, long value) {
    storage.writeLong(p + (6 * SIZE_T_SIZE), value);
  }
  
  private int index(long p) {
    return storage.readInt(p + (7 * SIZE_T_SIZE));
  }
  
  private void index(long p, int value) {
    storage.writeInt(p + (7 * SIZE_T_SIZE), value);
  }

  private long leftmostChild(long x) {
    long left = child(x, 0);
    return left != -1L ? left : child(x, 1);
  }
  
  private static long padRequest(long req) {
    return (req + CHUNK_OVERHEAD + CHUNK_ALIGN_MASK) & ~CHUNK_ALIGN_MASK;
  }

  private static long chunkToMem(long p) {
    return p + (SIZE_T_SIZE * 2);
  }
  
  private static long memToChunk(long p) {
    return p - (SIZE_T_SIZE * 2);
  }
  
  private static boolean okAddress(long a) {
    return a >= 0;
  }
  
  private static boolean okNext(long p, long n) {
    return p < n;
  }
  
  private static boolean isAligned(long a) {
    return (a & CHUNK_ALIGN_MASK) == 0;
  }
  
  private static long alignOffset(long a) {
    return (a & CHUNK_ALIGN_MASK) == 0 ? 0 : (MALLOC_ALIGNMENT - (a & CHUNK_ALIGN_MASK)) & CHUNK_ALIGN_MASK;
  }
  
  /* ---------------------------- Indexing Bins ---------------------------- */

  private static boolean isSmall(long s) {
    return smallBinIndex(s) < NSMALLBINS;
  }
  
  private static int smallBinIndex(long s) {
    return Integer.MAX_VALUE & (int) (s >>> SMALLBIN_SHIFT);
  }
  
  private static int smallBinIndexToSize(int i) {
    return i << SMALLBIN_SHIFT;
  }

  private static int treeBinIndex(long s)
  {
    int x = Integer.MAX_VALUE & (int) (s >>> TREEBIN_SHIFT);
    if (x == 0) {
      return 0;
    } else if (x > 0xFFFF) {
      return NTREEBINS - 1;
    } else {
      int k = 31 - Integer.numberOfLeadingZeros(x);
      return (k << 1) + (int) ((s >>> (k + (TREEBIN_SHIFT-1)) & 1));
    }
  }
  
  /* Mark/Clear bits with given index */
  private void markSmallMap(int i) {
    smallMap |= (1 << i);
  }
  
  private void clearSmallMap(int i) {
    smallMap &= ~(1 << i);
  }
  
  private boolean smallMapIsMarked(int i) {
    return (smallMap & (1 << i)) != 0;
  }

  private void markTreeMap(int i) {
    treeMap |= (1 << i);
  }
  
  private void clearTreeMap(int i) {
    treeMap  &= ~(1 << i);
  }
  
  private boolean treeMapIsMarked(int i) {
    return (treeMap & (1 << i)) != 0;
  }
  
  private static int leftShiftForTreeIndex(int i) {
    return (i == NTREEBINS-1) ? 0 : (SIZE_T_BITSIZE - 1) - ((i >>> 1) + TREEBIN_SHIFT - 2);
  }
  
  /* The size of the smallest chunk held in bin with index i */
  private static int minSizeForTreeIndex(int i) {
     return (1 << ((i >>> 1) + TREEBIN_SHIFT)) | ((i & 1) << ((i >>> 1) + TREEBIN_SHIFT - 1));
  }
  
  private static int leftBits(int i) {
    return (i << 1) | -(i << 1);
  }
  
  /** Debugging Support **/
  @Override
  public void validateAllocator() {
    if (topSize < 0) {
      return;
    }
    traverseAndCheck();
    for (int i = 0; i < smallBins.length; i++) {
      checkSmallBin(i);
    }
    for (int i = 0; i < treeBins.length; i++) {
      checkTreeBin(i);
    }
  }
  
  public long validateMallocedPointer(long m) {
    long p = memToChunk(m);

    checkMallocedChunk(m, chunkSize(p));
    if (findFreeChunk(p)) {
      throw new AssertionError();
    }
    return chunkSize(p);
  }
  
  /* Check properties of any chunk, whether free, inuse, mmapped etc  */
  private void checkAnyChunk(long p) {
    if (VALIDATING) {
      if (!isAligned(chunkToMem(p)))
        throw new AssertionError("Chunk address [mem:" + p + "=>chunk:" + chunkToMem(p) + "] is incorrectly aligned");
      if (!okAddress(p))
        throw new AssertionError("Memory address " + p + " is invalid");
    }
  }

  /* Check properties of top chunk */
  private void checkTopChunk(long p) {
    if (VALIDATING) {
      long sz = head(p) & ~INUSE_BITS; /* third-lowest bit can be set! */
      if (!isAligned(chunkToMem(p)))
        throw new AssertionError("Chunk address [mem:" + p + "=>chunk:" + chunkToMem(p) + "] of top chunk is incorrectly aligned");
      if (!okAddress(p))
        throw new AssertionError("Memory address " + p + " of top chunk is invalid");
      if (sz != topSize)
        throw new AssertionError("Marked size top chunk " + sz + " is not equals to the recorded top size " + topSize);
      if (sz <= 0)
        throw new AssertionError("Top chunk size " + sz + " is not positive");
      if (!previousInUse(p))
        throw new AssertionError("Chunk before top chunk is free - why has it not been merged in to the top chunk?");
    }
  }
  
  /* Check properties of inuse chunks */
  private void checkInUseChunk(long p) {
    if (VALIDATING) {
      checkAnyChunk(p);
      if (!isInUse(p))
        throw new AssertionError("Chunk at " + p + " is not in use");
      if (!nextPreviousInUse(p))
        throw new AssertionError("Chunk after " + p + " does not see this chunk as in use");
      /* If not pinuse previous chunk has OK offset */
      if (!previousInUse(p) && nextChunk(prevChunk(p)) != p)
        throw new AssertionError("Previous chunk to " + p + " is marked free but has an incorrect next pointer");
    }
  }

  /* Check properties of free chunks */
  private void checkFreeChunk(long p) {
    if (VALIDATING) {
      long sz = chunkSize(p);
      long next = p + sz;
      checkAnyChunk(p);
      if (isInUse(p))
        throw new AssertionError("Free chunk " + p + " is not marked as free");
      if (nextPreviousInUse(p))
        throw new AssertionError("Next chunk after " + p + " has it marked as in use");
      if (p != designatedVictim && p != top) {
        if (sz < MIN_CHUNK_SIZE) 
          throw new AssertionError("Free chunk " + p + " is too small");
        if ((sz & CHUNK_ALIGN_MASK) != 0)
          throw new AssertionError("Chunk size " + sz + " of " + p + " is not correctly aligned");
        if (!isAligned(chunkToMem(p)))
          throw new AssertionError("User pointer for chunk " + p + " is not correctly aligned");
        if (prevFoot(next) != sz)
          throw new AssertionError("Next chunk after " + p + " has an incorrect previous size");
        if (!previousInUse(p))
          throw new AssertionError("Chunk before free chunk " + p + " is free - should have been merged");
        if (next != top && !isInUse(next))
          throw new AssertionError("Chunk after free chunk " + p + " is free - should have been merged");        
        if (backward(forward(p)) != p)
          throw new AssertionError("Free chunk " + p + " has invalid chain links");
        if (forward(backward(p)) != p)
          throw new AssertionError("Free chunk " + p + " has invalid chain links");
      }
    }
  }
  
  /* Check properties of malloced chunks at the point they are malloced */
  private void checkMallocedChunk(long mem, long s) {
    if (VALIDATING) {
      long p = memToChunk(mem);
      long sz = head(p) & ~INUSE_BITS;
      checkInUseChunk(p);
      if (sz < MIN_CHUNK_SIZE) 
        throw new AssertionError("Allocated chunk " + p + " is too small");
      if ((sz & CHUNK_ALIGN_MASK) != 0)
        throw new AssertionError("Chunk size " + sz + " of " + p + " is not correctly aligned");
      if (sz < s)
        throw new AssertionError("Allocated chunk " + p + " is smaller than requested [" + sz + "<" + s + "]");
      if (sz > (s + MIN_CHUNK_SIZE)) {
        throw new AssertionError("Allocated chunk " + p + " is too large (should have been split off) [" + sz + ">>" + s + "]");
      }
    }
  }
  
  /* Check a tree and its subtrees.  */
  private void checkTree(long t) {
    long head = -1L;
    long u = t;
    int tindex = index(t);
    long tsize = chunkSize(t);
    int index = treeBinIndex(tsize);

    if (tindex != index) 
      throw new AssertionError("Tree node " + u + " has incorrect index [" + index(u) + "!=" + tindex + "]");
    if (tsize < MIN_LARGE_SIZE)
      throw new AssertionError("Tree node " + u + " is too small to be in a tree [" + tsize + "<" + MIN_LARGE_SIZE + "]");
    if (tsize < minSizeForTreeIndex(index))
      throw new AssertionError("Tree node " + u + " is too small to be in this tree [" + tsize + "<" + minSizeForTreeIndex(index) + "]");
    if (index != NTREEBINS-1 && tsize >= minSizeForTreeIndex(index + 1))
      throw new AssertionError("Tree node " + u + " is too large to be in this tree [" + tsize + ">=" + minSizeForTreeIndex(index + 1) + "]");

    do { /* traverse through chain of same-sized nodes */
      checkAnyChunk(u);
      if (index(u) != tindex)
        throw new AssertionError("Tree node " + u + " has incorrect index [" + index(u) + "!=" + tindex + "]");
      if (chunkSize(u) != tsize)
        throw new AssertionError("Tree node " + u + " has an mismatching size [" + chunkSize(u) + "!=" + tsize + "]");
      if (isInUse(u))
        throw new AssertionError("Tree node " + u + " is in use");
      if (nextPreviousInUse(u))
        throw new AssertionError("Tree node " + u + " is marked as in use in the next chunk");
      if (backward(forward(u)) != u)
        throw new AssertionError("Tree node " + u + " has incorrect chain links");
      if (forward(backward(u)) != u)
        throw new AssertionError("Tree node " + u + " has incorrect chain links");
      if (parent(u) == -1 && u != treeBins[index]) {
        if (child(u, 0) != -1)
          throw new AssertionError("Tree node " + u + " is chained from the tree but has a child " + child(u, 0));
        if (child(u, 1) != -1)
          throw new AssertionError("Tree node " + u + " is chained from the tree but has a child" + child(u, 1));
      } else {
        if (head != -1)
          throw new AssertionError("Tree node " + u + " is the second node in this chain with a parent [first was " + head + "]");
        head = u;
        
        if (treeBins[index] == u) {
          if (parent(u) != -1)
            throw new AssertionError("Tree node " + u + " is the head of the tree but has a parent " + parent(u));
        } else {
          if (parent(u) == u)
            throw new AssertionError("Tree node " + u + " is its own parent");
          if (child(parent(u), 0) != u && child(parent(u), 1) != u)
            throw new AssertionError("Tree node " + u + " is not a child of its parent");
        }
        
        if (child(u, 0) != -1) {
          if (parent(child(u, 0)) != u)
            throw new AssertionError("Tree node " + u + " is not the parent of its left child");
          if (child(u, 0) == u)
            throw new AssertionError("Tree node " + u + " is its own left child");
          checkTree(child(u, 0));
        }
        if (child(u, 1) != -1) {
          if (parent(child(u, 1)) != u)
            throw new AssertionError("Tree node " + u + " is not the parent of its right child");
          if (child(u, 1) == u)
            throw new AssertionError("Tree node " + u + " is its own right child");
          checkTree(child(u, 1));
        }
        if (child(u, 0) != -1 && child(u, 1) != -1 && chunkSize(child(u, 0)) >= chunkSize(child(u, 1))) {
          throw new AssertionError("Tree node " + u + " has it's left child bigger than it's right child");
        }
      }
      u = forward(u);
    } while (u != t);
    
    if (head == -1)
      throw new AssertionError("This tree level has no nodes with a parent");
  }

  /*  Check all the chunks in a treebin.  */
  private void checkTreeBin(int index) {
    long tb = treeBins[index];

    boolean empty = (treeMap & (1 << index)) == 0;
    if (tb == -1 && !empty) {
      throw new AssertionError("Tree " + index + " is marked as occupied but has an invalid head pointer");
    }
    if (!empty) {
      checkTree(tb);
    }
  }

  /*  Check all the chunks in a smallbin.  */
  private void checkSmallBin(int index) {
    long h = smallBins[index];
    
    boolean empty = (smallMap & (1 << index)) == 0;
    if (h == -1 && !empty) {
      throw new AssertionError("Small bin chain " + index + " is marked as occupied but has an invalid head pointer");
    }
    if (!empty) {
      long p = h;
      do {
        long size = chunkSize(p);
        /* each chunk claims to be free */
        checkFreeChunk(p);
        /* chunk belongs in bin */
        if (smallBinIndex(size) != index)
          throw new AssertionError("Chunk " + p + " is the wrong size to be in bin " + index);
        if (backward(p) != p && chunkSize(backward(p)) != chunkSize(p))
          throw new AssertionError("Chunk " + p + " is the linked to a chunk of the wrong size");
        /* chunk is followed by an inuse chunk */
        checkInUseChunk(nextChunk(p));
        
        p = backward(p);
      } while (p != h);
    }
  }

  /* Traverse each chunk and check it; return total */
  private long traverseAndCheck() {
    long sum = 0;

    sum += topSize + TOP_FOOT_SIZE;

    long q = 0;
    long lastq = 0;
    if (!previousInUse(q))
      throw new AssertionError("Chunk before zeroth chunk is marked as free");
    while (q != top) {
      sum += chunkSize(q);
      if (isInUse(q)) {
        if (findFreeChunk(q))
          throw new AssertionError("Chunk marked as in-use appears in a free structure");
        checkInUseChunk(q);
      } else {
        if (q != designatedVictim && !findFreeChunk(q))
          throw new AssertionError("Chunk marked as free cannot be found in any free structure");
        if (lastq != 0 && !isInUse(lastq))
          throw new AssertionError("Chunk before free chunk is not in-use");
        checkFreeChunk(q);
      }
      lastq = q;
      q = nextChunk(q);
    }
    return sum;
  }
  
  /* Find x in a bin. Used in other check functions. */
  private boolean findFreeChunk(long x) {
    long size = chunkSize(x);
    if (isSmall(size)) {
      int sidx = smallBinIndex(size);
      long h = smallBins[sidx];
      if (smallMapIsMarked(sidx)) {
        long p = h;
        do {
          if (p == x)
            return true;
        } while ((p = forward(p)) != h);
      }
    } else {
      int tidx = treeBinIndex(size);
      if (treeMapIsMarked(tidx)) {
        long t = treeBins[tidx];
        long sizebits = size << leftShiftForTreeIndex(tidx);
        while (t != -1L && chunkSize(t) != size) {
          t = child(t, (int) ((sizebits >>> (SIZE_T_BITSIZE - 1)) & 1));
          sizebits <<= 1;
        }
        if (t != -1L) {
          long u = t;
          do {
            if (u == x)
              return true;
          } while ((u = forward(u)) != t);
        }
      }
    }
    return false;
  }

  @Override
  public long getLastUsedPointer() {
    if (top <= 0) {
      return -1L;
    } else {
      return chunkToMem(prevChunk(top));
    }
  }

  @Override
  public long getLastUsedAddress() {
    if (top <= 0) {
      return 0;
    } else {
      return top;
    }
  }
  
  @Override
  public int getMinimalSize() {
    return (int) MINIMAL_SIZE;
  }
  
  @Override
  public long getMaximumAddress() {
    return Long.MAX_VALUE;
  }

  @Override
  public Iterator<Long> iterator() {
    return new Iterator<Long>() {

      private long current = (isInUse(0) || 0 >= top) ? 0 : nextChunk(0);
      
      @Override
      public boolean hasNext() {
        return current < top;
      }

      @Override
      public Long next() {
        if (current >= top) {
          throw new NoSuchElementException();
        } else {
          long result = current;
          long next = nextChunk(current);
          if (next >= top) {
            current = next;
          } else {
            current = isInUse(next) ? next : nextChunk(next);
          }
          return chunkToMem(result);
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };
  }
}
