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
package org.terracotta.offheapstore.util;

import java.util.Iterator;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Tests verifying issue fixes in {@link AATreeSet}.
 *
 * <h3>Issue Description</h3>
 * When iterating over a subset view (such as {@code tailSet(E start)}) of an empty
 * {@link AATreeSet}, creating the iterator triggers a {@link NullPointerException}.
 *
 * <h3>Root Cause</h3>
 * In {@code AATreeSet.TreeIterator(T start)}, the constructor traverses the tree starting
 * at {@code root} to find the subtree root matching {@code start}:
 * <pre>{@code
 *   Node<T> current = root;
 *   while (!current.isTerminal()) {
 *     if (current.getPayload().compareTo(start) < 0) { ... }
 *   }
 * }</pre>
 * However, before entering the traversal loop, it unconditionally reads:
 * <pre>{@code
 *   T currentPayload = current.getPayload();
 * }</pre>
 * On an empty tree, {@code root} is {@code TerminalNode.terminal()}, whose {@code getPayload()}
 * returns {@code null}, leading directly to {@link NullPointerException} or crashes when
 * calling {@code currentPayload.compareTo(start)}.
 *
 * <h3>Expected Behavior</h3>
 * When the tree is empty (i.e. {@code root.isTerminal()}), {@code TreeIterator} should initialize
 * to an empty iterator immediately without attempting to dereference {@code root.getPayload()}.
 */
public class AATreeSetTest {

  @Test
  public void testEmptyTailSetIteratorDoesNotThrowNPE() {
    AATreeSet<Integer> tree = new AATreeSet<>();

    // tailSet on an empty tree must return an iterator where hasNext() is false,
    // rather than throwing NullPointerException during iterator construction.
    Iterator<Integer> it = tree.tailSet(10).iterator();
    assertThat(it.hasNext(), is(false));
  }
}
