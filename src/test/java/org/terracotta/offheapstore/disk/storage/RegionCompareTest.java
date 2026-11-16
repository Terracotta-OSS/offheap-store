/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2026
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
package org.terracotta.offheapstore.disk.storage;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Tests verifying issue fixes in {@link Region}.
 *
 * <h3>Issue 1: {@code Comparable.compareTo} Antisymmetry Contract Violation</h3>
 * <p>
 * The Java {@link Comparable} interface strictly requires antisymmetry:
 * {@code sgn(x.compareTo(y)) == -sgn(y.compareTo(x))}.
 * </p>
 * <p>
 * In {@link Region#compareTo(Region)}, when two regions have the exact same start offset
 * but different end offsets (e.g. {@code a = [10, 20]} and {@code b = [10, 30]}):
 * <ul>
 *   <li>{@code a.compareTo(b)} evaluates {@code a.end <= b.end} (20 <= 30), returning {@code 0} (treated as equal).</li>
 *   <li>{@code b.compareTo(a)} evaluates {@code b.start >= a.end} (10 >= 20 -> false) and then
 *       {@code b.end <= a.end} (30 <= 20 -> false), finally falling through to return {@code 1}.</li>
 * </ul>
 * This results in {@code a.compareTo(b) == 0} while {@code b.compareTo(a) == 1}, violating the
 * {@link Comparable} contract. When stored in search trees (like {@link org.terracotta.offheapstore.util.AATreeSet}),
 * tree balancing (skew/split) can corrupt ordering and cause region search/deallocation lookups to fail or loop.
 *
 * <h3>Issue 2: Stale Partner BitSet on {@code swapPayload}</h3>
 * <p>
 * In {@link Region#swapPayload(org.terracotta.offheapstore.util.AATreeSet.Node)}, when balancing or rotating
 * nodes in the AA-Tree, {@code this.swapPayload(r)} exchanges the start and end offsets between two regions.
 * After swapping coordinates, {@code this.updateAvailable()} is called, updating {@code this}'s cached
 * available bitmask. However, {@code r.updateAvailable()} is omitted!
 * </p>
 * <p>
 * As a consequence, {@code r} retains the stale bitmask calculated from its prior coordinates,
 * leading to corrupted allocator bitmap tracking and erroneous disk allocation decisions.
 * </p>
 */
public class RegionCompareTest {

  @Test
  public void testCompareToSymmetryWithSameStartDifferentEnd() {
    Region a = new Region(10, 20);
    Region b = new Region(10, 30);

    int cmpAB = a.compareTo(b);
    int cmpBA = b.compareTo(a);

    // Antisymmetry contract check: sgn(a.compareTo(b)) must equal -sgn(b.compareTo(a))
    assertThat("sgn(a.compareTo(b)) must equal -sgn(b.compareTo(a))",
        Integer.signum(cmpAB), is(-Integer.signum(cmpBA)));
  }

  @Test
  public void testSwapPayloadUpdatesPartnerBitset() {
    Region a = new Region(0, 15);
    Region b = new Region(100, 107);

    // Swap payloads between nodes a and b
    a.swapPayload(b);

    // After swapping, b now holds range [0, 15] so its available bitset must reflect b.availableHere() ([0, 15]),
    // and a now holds [100, 107] so its available bitset must reflect a.availableHere() ([100, 107]).
    assertThat("Partner node bitmask must be updated after payload swap",
        b.available(), is(b.availableHere()));
    assertThat("This node bitmask must be updated after payload swap",
        a.available(), is(a.availableHere()));
  }
}
