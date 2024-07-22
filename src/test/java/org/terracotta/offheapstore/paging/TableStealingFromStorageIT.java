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
package org.terracotta.offheapstore.paging;

import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.paging.PageSource;
import static org.terracotta.offheapstore.util.MemoryUnit.KILOBYTES;
import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.AbstractXYDataset;
import org.junit.Ignore;
import org.junit.Test;

import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.WriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.storage.portability.ByteArrayPortability;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TableStealingFromStorageIT {

  @Test
  public void testStealingFromOwnStorage() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), MEGABYTES.toBytes(1), MEGABYTES.toBytes(1));

    OffHeapHashMap<Integer, byte[]> selfStealer = new WriteLockedOffHeapClockCache<>(source, true, new SplitStorageEngine<>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<>(source, KILOBYTES
      .toBytes(16), ByteArrayPortability.INSTANCE, false, true)));

    long seed = System.nanoTime();
    System.err.println("Random Seed = " + seed);
    Random rndm = new Random(seed);

    int terminalKey = 0;
    for (int key = 0; true; key++) {
      int size = selfStealer.size();
      byte[] payload = new byte[rndm.nextInt(KILOBYTES.toBytes(1))];
      Arrays.fill(payload, (byte) key);
      selfStealer.put(key, payload);
      if (size >= selfStealer.size()) {
        terminalKey = key;
        break;
      }
    }

    System.err.println("Terminal Key = " + terminalKey);

    for (int key = terminalKey + 1; key < 10 * terminalKey; key++) {
      byte[] payload = new byte[rndm.nextInt(KILOBYTES.toBytes(1))];
      Arrays.fill(payload, (byte) key);
      selfStealer.put(key, payload);
      selfStealer.remove(rndm.nextInt(key));
    }

    long measuredSize = 0;
    for (Entry<Integer, byte[]> e : selfStealer.entrySet()) {
      int key = e.getKey();
      byte[] payload = e.getValue();
      for (byte b : payload) {
        assertThat(b, is((byte) key));
      }
      assertThat(selfStealer, hasKey(e.getKey()));
      measuredSize++;
    }
    assertThat(selfStealer.size(), is((int) measuredSize));
    assertThat(selfStealer.getUsedSlotCount(), is(measuredSize));
  }

  @Test
  @Ignore
  public void testSelfStealingIsStable() throws IOException {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), MEGABYTES.toBytes(2), MEGABYTES.toBytes(1));

    OffHeapHashMap<Integer, byte[]> selfStealer = new WriteLockedOffHeapClockCache<>(source, true, new SplitStorageEngine<>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<>(source, KILOBYTES
      .toBytes(16), ByteArrayPortability.INSTANCE, false, true)));

    // failing seed value
    //long seed = 1302292028471110000L;

    long seed = System.nanoTime();
    System.err.println("Random Seed = " + seed);
    Random rndm = new Random(seed);
    int payloadSize = rndm.nextInt(KILOBYTES.toBytes(1));
    System.err.println("Payload Size = " + payloadSize);

    List<Long> sizes = new ArrayList<>();
    List<Long> tableSizes = new ArrayList<>();

    int terminalKey = 0;
    for (int key = 0; true; key++) {
      int size = selfStealer.size();
      byte[] payload = new byte[payloadSize];
      Arrays.fill(payload, (byte) key);
      selfStealer.put(key, payload);
      sizes.add((long) selfStealer.size());
      tableSizes.add(selfStealer.getTableCapacity());
      if (size >= selfStealer.size()) {
        terminalKey = key;
        selfStealer.remove(terminalKey);
        break;
      }
    }

    System.err.println("Terminal Key = " + terminalKey);

    int shrinkCount = 0;
    for (int key = terminalKey; key < 10 * terminalKey; key++) {
      byte[] payload = new byte[payloadSize];
      Arrays.fill(payload, (byte) key);
      long preTableSize = selfStealer.getTableCapacity();
      selfStealer.put(key, payload);
      if (rndm.nextBoolean()) {
        selfStealer.remove(rndm.nextInt(key));
      }
      long postTableSize = selfStealer.getTableCapacity();
      if (preTableSize > postTableSize) {
        shrinkCount++;
      }
      sizes.add((long) selfStealer.size());
      tableSizes.add(postTableSize);
    }

    if (shrinkCount != 0) {
      Map<String, List<? extends Number>> data = new HashMap<>();
      data.put("actual size", sizes);
      data.put("table size", tableSizes);

      JFreeChart chart = ChartFactory.createXYLineChart("Cache Size", "operations", "size", new LongListXYDataset(data), PlotOrientation.VERTICAL, true, false, false);
      File plotOutput = new File("target/TableStealingFromStorageTest.testSelfStealingIsStable.png");
      ChartUtilities.saveChartAsPNG(plotOutput, chart, 640, 480);
      fail("Expected no shrink events after reaching equilibrium : saw " + shrinkCount + " [plot in " + plotOutput + "]");
    }
  }

  static class LongListXYDataset extends AbstractXYDataset {

    private static final long serialVersionUID = 1119808858992366568L;

    private final List<String> keys;
    private final List<List<? extends Number>> data;

    public LongListXYDataset(Map<String, List<? extends Number>> series) {
      keys = new ArrayList<>(series.size());
      data = new ArrayList<>(series.size());

      for (Entry<String, List<? extends Number>> e : series.entrySet()) {
        keys.add(e.getKey());
        data.add(e.getValue());
      }
    }

    @Override
    public int getItemCount(int seriesIndex) {
      return data.get(seriesIndex).size();
    }

    @Override
    public Number getX(int seriesIndex, int dataIndex) {
      return dataIndex;
    }

    @Override
    public Number getY(int seriesIndex, int dataIndex) {
      return data.get(seriesIndex).get(dataIndex);
    }

    @Override
    public int getSeriesCount() {
      return keys.size();
    }

    @Override
    public Comparable<?> getSeriesKey(int seriesIndex) {
      return keys.get(seriesIndex);
    }

  }
}
