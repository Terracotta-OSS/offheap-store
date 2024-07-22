/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import java.util.Iterator;
import java.util.NoSuchElementException;
import static org.terracotta.offheapstore.util.Validation.shouldValidate;
import static org.terracotta.offheapstore.util.Validation.validate;

/**
 * An aggressively best-fit allocator.
 *
 * @author Chris Dennis
 */
public final class IntegerBestFitAllocator implements Allocator {

  private static final boolean DEBUG = Boolean.getBoolean(IntegerBestFitAllocator.class.getName() + ".DEBUG");
  private static final boolean VALIDATING = shouldValidate(IntegerBestFitAllocator.class);

  /* The byte and bit size of a size_t */
  private static final int SIZE_T_SIZE       = Integer.SIZE / Byte.SIZE;
  private static final int SIZE_T_BITSIZE    = Integer.SIZE;

  private static final int MALLOC_ALIGNMENT = 2 * SIZE_T_SIZE;

  /* The bit mask value corresponding to MALLOC_ALIGNMENT */
  private static final int CHUNK_ALIGN_MASK  = MALLOC_ALIGNMENT - 1;

  /* ------------------- Chunks sizes and alignments ----------------------- */
  private static final int MCHUNK_SIZE       = 4 * SIZE_T_SIZE;

  private static final int CHUNK_OVERHEAD    = 2 * SIZE_T_SIZE;

  /* The smallest size we can malloc is an aligned minimal chunk */
  private static final int MIN_CHUNK_SIZE    = (MCHUNK_SIZE + CHUNK_ALIGN_MASK) & ~CHUNK_ALIGN_MASK;

  /* Bounds on request (not chunk) sizes. */
  private static final int MAX_REQUEST       = ((-MIN_CHUNK_SIZE) << 2) & Integer.MAX_VALUE;
  private static final int MIN_REQUEST       = MIN_CHUNK_SIZE - CHUNK_OVERHEAD - 1;

  private static final int TOP_FOOT_SIZE     = alignOffset(chunkToMem(0)) + padRequest(MIN_CHUNK_SIZE);

  private static final int MINIMAL_SIZE       = Integer.highestOneBit(TOP_FOOT_SIZE) << 1;

  private static final int TOP_FOOT_OFFSET    = memToChunk(0) + TOP_FOOT_SIZE;

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

  private final int[] smallBins = new int[NSMALLBINS];
  private final int[] treeBins = new int[NTREEBINS];

  private int designatedVictimSize = 0;
  private int designatedVictim = -1;

  private int topSize = 0;
  private int top = 0;

  private int occupied;

  /**
   * Create a best fit allocator backed by the given OffHeapStorageArea.
   *
   * @param storage source of ByteBuffer instances
   */
  public IntegerBestFitAllocator(OffHeapStorageArea storage) {
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
    return dlmalloc((int) size);
  }

  @Override
  public void free(long address) {
    dlfree((int) address, true);
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

    int q = 0;
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

  private int dlmalloc(int bytes) {
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

    int nb = (bytes < MIN_REQUEST) ? MIN_CHUNK_SIZE : padRequest(bytes); //internal request size;

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
      return -1;
    } else if (treeMap != 0) {
      int mem = splitFromTree(nb);
      if (okAddress(mem)) {
        return mem;
      }
    }

    if (nb <= designatedVictimSize) {
      return splitFromDesignatedVictim(nb);
    } else if (nb < topSize) {
      return splitFromTop(nb);
    }

    return -1;
  }

  private int allocateFromSmallBin(int index, int nb) {
    int h = smallBins[index];
    validate(!VALIDATING || chunkSize(h) == smallBinIndexToSize(index));

    int f = forward(h);
    int b = backward(h);

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
    int mem = chunkToMem(h);
    checkMallocedChunk(mem, nb);
    return mem;
  }

  private int splitFromSmallBin(int index, int nb) {
    int h = smallBins[index];
    validate(!VALIDATING || chunkSize(h) == smallBinIndexToSize(index));

    int f = forward(h);
    int b = backward(h);

    if (f == h) {
      validate(!VALIDATING || b == h);
      clearSmallMap(index);
      smallBins[index] = -1;
    } else {
      smallBins[index] = f;
      backward(f, b);
      forward(b, f);
    }

    int rsize = smallBinIndexToSize(index) - nb;

    /* Fit here cannot be remainderless if 4byte sizes */
    if (rsize < MIN_CHUNK_SIZE) {
      setInUseAndPreviousInUse(h, smallBinIndexToSize(index));
    } else {
      setSizeAndPreviousInUseOfInUseChunk(h, nb);
      int r = h + nb;
      setSizeAndPreviousInUseOfFreeChunk(r, rsize);
      replaceDesignatedVictim(r, rsize);
    }

    int mem = chunkToMem(h);
    checkMallocedChunk(mem, nb);
    return mem;
  }

  private void replaceDesignatedVictim(int p, int s) {
    int dvs = designatedVictimSize;
    if (dvs != 0) {
      int dv = designatedVictim;
      validate(!VALIDATING || isSmall(dvs));
      insertSmallChunk(dv, dvs);
    }
    designatedVictimSize = s;
    designatedVictim = p;
  }

  private int splitSmallFromTree(int nb) {
    int index = Integer.numberOfTrailingZeros(treeMap);

    int t;
    int v = t = treeBins[index];
    int rsize = chunkSize(t) - nb;

    while ((t = leftmostChild(t)) != -1) {
      int trem = chunkSize(t) - nb;
      if (trem >= 0 && trem < rsize) {
        rsize = trem;
        v = t;
      }
    }

    if (okAddress(v)) {
      int r = v + nb;
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
        int mem = chunkToMem(v);
        checkMallocedChunk(mem, nb);
        return mem;
      } else {
        throw new AssertionError();
      }
    } else {
      throw new AssertionError();
    }
  }

  private int splitFromTree(int nb) {
    int v = -1;
    int rsize = Integer.MAX_VALUE & (-nb); /* Unsigned negation */
    int t;

    int index = treeBinIndex(nb);

    if ((t = treeBins[index]) != -1) {
      /* Traverse tree for this bin looking for node with size == nb */
      int sizebits = nb << leftShiftForTreeIndex(index);
      int rst = -1;  /* The deepest untaken right subtree */
      for (;;) {
        int rt;
        int trem = chunkSize(t) - nb;
        if (trem >= 0 && trem < rsize) {
          v = t;
          if ((rsize = trem) == 0)
            break;
        }
        rt = child(t, 1);
        t = child(t, (sizebits >>> (SIZE_T_BITSIZE - 1)));
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
      int trem = chunkSize(t) - nb;
      if (trem >= 0 && trem < rsize) {
        rsize = trem;
        v = t;
      }
      t = leftmostChild(t);
    }

    /*  If dv is a better fit, return 0 so malloc will use it */
    int designatedVictimFit = designatedVictimSize - nb;
    if (v != -1 && (designatedVictimFit < 0 || rsize < designatedVictimFit)) {
      if (okAddress(v)) { /* split */
        int r = v + nb;
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
    return -1;
  }

  private int splitFromDesignatedVictim(int nb) {
    int rsize = designatedVictimSize - nb;
    int p = designatedVictim;

    if (rsize >= MIN_CHUNK_SIZE) { /* split dv */
      int r = designatedVictim = p + nb;
      designatedVictimSize = rsize;
      setSizeAndPreviousInUseOfFreeChunk(r, rsize);
      setSizeAndPreviousInUseOfInUseChunk(p, nb);
    } else { /* exhaust dv */
      int dvs = designatedVictimSize;
      designatedVictimSize = 0;
      designatedVictim = -1;
      setInUseAndPreviousInUse(p, dvs);
    }
    int mem = chunkToMem(p);
    checkMallocedChunk(mem, nb);
    return mem;
  }

  private int splitFromTop(int nb) {
    int rSize = topSize -= nb;
    int p = top;
    int r = top = p + nb;
    head(r, rSize | PINUSE_BIT);
    setSizeAndPreviousInUseOfInUseChunk(p, nb);
    int mem = chunkToMem(p);
    checkTopChunk(top);
    checkMallocedChunk(mem, nb);
    return mem;
  }

  private void dlfree(int mem, boolean shrink) {
    /*
     * Consolidate freed chunks with preceeding or succeeding bordering free
     * chunks, if they exist, and then place in a bin. Intermixed with special
     * cases for top, designatedVictim, and usage errors.
     */
    int p = memToChunk(mem);

    if (okAddress(p) && isInUse(p)) {
      checkInUseChunk(p);
      int psize = chunkSize(p);
      occupied -= psize;
      int next = p + psize;

      if (!previousInUse(p)) {
        int previousSize = prevFoot(p);

        int previous = p - previousSize;
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
            int tsize = topSize += psize;
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
            int dsize = designatedVictimSize += psize;
            designatedVictim = p;
            setSizeAndPreviousInUseOfFreeChunk(p, dsize);
            return;
          } else {
            int nsize = chunkSize(next);
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

  private void insertChunk(int p, int s) {
    if (isSmall(s)) {
      insertSmallChunk(p, s);
    } else {
      insertLargeChunk(p, s);
    }
  }

  private void insertSmallChunk(int p, int s) {
    int index = smallBinIndex(s);

    int h = smallBins[index];

    if (!smallMapIsMarked(index)) {
      markSmallMap(index);
      smallBins[index] = p;
      forward(p, p);
      backward(p, p);
    } else if (okAddress(h)) {
      int b = backward(h);
      forward(b, p);
      forward(p, h);
      backward(h, p);
      backward(p, b);
    } else {
      throw new AssertionError();
    }
    checkFreeChunk(p);
  }

  private void insertLargeChunk(int x, int s) {
    int index = treeBinIndex(s);
    int h = treeBins[index];


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
      int t = h;
      int k = s << leftShiftForTreeIndex(index);
      for (;;) {
        if (chunkSize(t) != s) {
          int childIndex = (k >>> (SIZE_T_BITSIZE - 1)) & 1;
          int child = child(t, childIndex);
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
          int f = forward(t);
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

  private void unlinkChunk(int p, int s) {
    if (isSmall(s)) {
      unlinkSmallChunk(p, s);
    } else {
      unlinkLargeChunk(p);
    }
  }

  private void unlinkSmallChunk(int p, int s) {
    int f = forward(p);
    int b = backward(p);

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
  private void unlinkLargeChunk(int x) {
    int xp = parent(x);
    int r;
    if (backward(x) != x) {
      int f = forward(x);
      r = backward(x);
      if (okAddress(f)) {
        backward(f, r);
        forward(r, f);
      } else {
        throw new AssertionError();
      }
    } else {
      int rpIndex;
      if (((r = child(x, rpIndex = 1)) != -1) ||
          ((r = child(x, rpIndex = 0)) != -1)) {
        int rp = x;
        while (true) {
          if (child(r, 1) != -1) {
            rp = r;
            rpIndex = 1;
            r = child(r, 1);
          } else if (child(r, 0) != -1) {
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
    if (xp != -1 || treeBins[index] == x) {
      int h = treeBins[index];
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
          int c0, c1;
          parent(r, xp);
          if ((c0 = child(x, 0)) != -1) {
            if (okAddress(c0)) {
              child(r, 0, c0);
              parent(c0, r);
            } else {
              throw new AssertionError();
            }
          }
          if ((c1 = child(x, 1)) != -1) {
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
  private boolean chunkInUse(int p) {
    return (head(p) & CINUSE_BIT) != 0;
  }

  private boolean previousInUse(int p) {
    return (head(p) & PINUSE_BIT) != 0;
  }

  private boolean isInUse(int p) {
    return (head(p) & INUSE_BITS) != PINUSE_BIT;
  }

  private int chunkSize(int p) {
    return head(p) & ~FLAG_BITS;
  }

  private void clearPreviousInUse(int p) {
    head(p, head(p) & ~PINUSE_BIT);
  }

  /* Ptr to next or previous physical malloc_chunk. */
  private int nextChunk(int p) {
    return p + chunkSize(p);
  }

  private int prevChunk(int p) {
    return p - prevFoot(p);
  }

  /* extract next chunk's pinuse bit */
  private boolean nextPreviousInUse(int p) {
    return (head(nextChunk(p)) & PINUSE_BIT) != 0;
  }

  /* Set size at footer */
  private void setFoot(int p, int s) {
    prevFoot(p + s, s);
  }

  /* Set size, pinuse bit, and foot */
  private void setSizeAndPreviousInUseOfFreeChunk(int p, int s) {
    head(p, s | PINUSE_BIT);
    setFoot(p, s);
  }

  /* Set size, pinuse bit, foot, and clear next pinuse */
  private void setFreeWithPreviousInUse(int p, int s, int n) {
    clearPreviousInUse(n);
    setSizeAndPreviousInUseOfFreeChunk(p, s);
  }

  /* Set cinuse and pinuse of this chunk and pinuse of next chunk */
  private void setInUseAndPreviousInUse(int p, int s) {
    setSizeAndPreviousInUseOfInUseChunk(p, s);
    head(p + s, head(p + s) | PINUSE_BIT);
  }

  /* Set size, cinuse and pinuse bit of this chunk */
  private void setSizeAndPreviousInUseOfInUseChunk(int p, int s) {
    head(p, s | PINUSE_BIT | CINUSE_BIT);
    setFoot(p, s);
    occupied += s;
  }

  private int prevFoot(int p) {
    return storage.readInt(p);
  }

  private void prevFoot(int p, int value) {
    storage.writeInt(p, value);
  }

  private int head(int p) {
    return storage.readInt(p + 4);
  }

  private void head(int p, int value) {
    storage.writeInt(p + 4, value);
  }

  private int forward(int p) {
    return storage.readInt(p + 8);
  }

  private void forward(int p, int value) {
    storage.writeInt(p + 8, value);
  }

  private int backward(int p) {
    return storage.readInt(p + 12);
  }

  private void backward(int p, int value) {
    storage.writeInt(p + 12, value);
  }

  private int child(int p, int index) {
    return storage.readInt(p + 16 + (4 * index));
  }

  private void child(int p, int index, int value) {
    storage.writeInt(p + 16 + (4 * index), value);
  }

  private int parent(int p) {
    return storage.readInt(p + 24);
  }

  private void parent(int p, int value) {
    storage.writeInt(p + 24, value);
  }

  private int index(int p) {
    return storage.readInt(p + 28);
  }

  private void index(int p, int value) {
    storage.writeInt(p + 28, value);
  }

  private int leftmostChild(int x) {
    int left = child(x, 0);
    return left != -1 ? left : child(x, 1);
  }

  private static int padRequest(int req) {
    return (req + CHUNK_OVERHEAD + CHUNK_ALIGN_MASK) & ~CHUNK_ALIGN_MASK;
  }

  private static int chunkToMem(int p) {
    return p + (SIZE_T_SIZE * 2);
  }

  private static int memToChunk(int p) {
    return p - (SIZE_T_SIZE * 2);
  }

  private static boolean okAddress(int a) {
    return a >= 0;
  }

  private static boolean okNext(int p, int n) {
    return p < n;
  }

  private static boolean isAligned(int a) {
    return (a & CHUNK_ALIGN_MASK) == 0;
  }

  private static int alignOffset(int a) {
    return (a & CHUNK_ALIGN_MASK) == 0 ? 0 : (MALLOC_ALIGNMENT - (a & CHUNK_ALIGN_MASK)) & CHUNK_ALIGN_MASK;
  }

  /* ---------------------------- Indexing Bins ---------------------------- */

  private static boolean isSmall(int s) {
    return smallBinIndex(s) < NSMALLBINS;
  }

  private static int smallBinIndex(int s) {
    return s >>> SMALLBIN_SHIFT;
  }

  private static int smallBinIndexToSize(int i) {
    return i << SMALLBIN_SHIFT;
  }

  private static int treeBinIndex(int s)
  {
    int x = s >>> TREEBIN_SHIFT;
    if (x == 0) {
      return 0;
    } else if (x > 0xFFFF) {
      return NTREEBINS - 1;
    } else {
      int k = 31 - Integer.numberOfLeadingZeros(x);
      return (k << 1) + ((s >>> (k + (TREEBIN_SHIFT-1)) & 1));
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

  public int validateMallocedPointer(int m) {
    int p = memToChunk(m);

    checkMallocedChunk(m, chunkSize(p));
    if (findFreeChunk(p)) {
      throw new AssertionError();
    }
    return chunkSize(p);
  }

  /* Check properties of any chunk, whether free, inuse, mmapped etc  */
  private void checkAnyChunk(int p) {
    if (VALIDATING) {
      if (!isAligned(chunkToMem(p)))
        throw new AssertionError("Chunk address [mem:" + p + "=>chunk:" + chunkToMem(p) + "] is incorrectly aligned");
      if (!okAddress(p))
        throw new AssertionError("Memory address " + p + " is invalid");
    }
  }

  /* Check properties of top chunk */
  private void checkTopChunk(int p) {
    if (VALIDATING) {
      int  sz = head(p) & ~INUSE_BITS; /* third-lowest bit can be set! */
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
  private void checkInUseChunk(int p) {
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
  private void checkFreeChunk(int p) {
    if (VALIDATING) {
      int sz = chunkSize(p);
      int next = p + sz;
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
  private void checkMallocedChunk(int mem, int s) {
    if (VALIDATING) {
      int p = memToChunk(mem);
      int sz = head(p) & ~INUSE_BITS;
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
  private void checkTree(int t) {
    int head = -1;
    int u = t;
    int tindex = index(t);
    int tsize = chunkSize(t);
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
    int tb = treeBins[index];

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
    int h = smallBins[index];

    boolean empty = (smallMap & (1 << index)) == 0;
    if (h == -1 && !empty) {
      throw new AssertionError("Small bin chain " + index + " is marked as occupied but has an invalid head pointer");
    }
    if (!empty) {
      int p = h;
      do {
        int size = chunkSize(p);
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
  private int traverseAndCheck() {
    int sum = 0;

    sum += topSize + TOP_FOOT_SIZE;

    int q = 0;
    int lastq = 0;
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
  private boolean findFreeChunk(int x) {
    int size = chunkSize(x);
    if (isSmall(size)) {
      int sidx = smallBinIndex(size);
      int h = smallBins[sidx];
      if (smallMapIsMarked(sidx)) {
        int p = h;
        do {
          if (p == x)
            return true;
        } while ((p = forward(p)) != h);
      }
    } else {
      int tidx = treeBinIndex(size);
      if (treeMapIsMarked(tidx)) {
        int t = treeBins[tidx];
        int sizebits = size << leftShiftForTreeIndex(tidx);
        while (t != -1 && chunkSize(t) != size) {
          t = child(t, (sizebits >>> (SIZE_T_BITSIZE - 1)) & 1);
          sizebits <<= 1;
        }
        if (t != -1) {
          int u = t;
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
      return -1;
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
    return MINIMAL_SIZE;
  }

  @Override
  public long getMaximumAddress() {
    return Integer.MAX_VALUE;
  }

  @Override
  public Iterator<Long> iterator() {
    return new Iterator<Long>() {

      private int current = (isInUse(0) || 0 >= top) ? 0 : nextChunk(0);

      @Override
      public boolean hasNext() {
        return current < top;
      }

      @Override
      public Long next() {
        if (current >= top) {
          throw new NoSuchElementException();
        } else {
          int result = current;
          int next = nextChunk(current);
          if (next >= top) {
            current = next;
          } else {
            current = isInUse(next) ? next : nextChunk(next);
          }
          return (long) chunkToMem(result);
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
