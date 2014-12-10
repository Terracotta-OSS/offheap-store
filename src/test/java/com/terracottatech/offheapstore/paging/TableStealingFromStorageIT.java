package com.terracottatech.offheapstore.paging;

import static com.terracottatech.offheapstore.util.MemoryUnit.KILOBYTES;
import static com.terracottatech.offheapstore.util.MemoryUnit.MEGABYTES;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import junit.framework.Assert;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.AbstractXYDataset;
import org.junit.Ignore;
import org.junit.Test;

import com.terracottatech.offheapstore.OffHeapHashMap;
import com.terracottatech.offheapstore.WriteLockedOffHeapClockCache;
import com.terracottatech.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;

public class TableStealingFromStorageIT {

  @Test
  public void testStealingFromOwnStorage() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), MEGABYTES.toBytes(1), MEGABYTES.toBytes(1));

    OffHeapHashMap<Integer, byte[]> selfStealer = new WriteLockedOffHeapClockCache<Integer, byte[]>(source, true, new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, KILOBYTES.toBytes(16), ByteArrayPortability.INSTANCE, false, true)));

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
    
    int measuredSize = 0;
    for (Entry<Integer, byte[]> e : selfStealer.entrySet()) {
      int key = e.getKey();
      byte[] payload = e.getValue();
      for (byte b : payload) {
        Assert.assertEquals((byte) key, b);
      }
      Assert.assertTrue(selfStealer.containsKey(e.getKey()));
      Assert.assertNotNull(selfStealer.get(e.getKey()));
      measuredSize++;
    }
    Assert.assertEquals(measuredSize, selfStealer.size());
    Assert.assertEquals(selfStealer.size(), selfStealer.getUsedSlotCount());
  }
  
  @Test
  @Ignore
  public void testSelfStealingIsStable() throws IOException {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), MEGABYTES.toBytes(2), MEGABYTES.toBytes(1));

    OffHeapHashMap<Integer, byte[]> selfStealer = new WriteLockedOffHeapClockCache<Integer, byte[]>(source, true, new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, KILOBYTES.toBytes(16), ByteArrayPortability.INSTANCE, false, true)));

    // failing seed value
    //long seed = 1302292028471110000L;

    long seed = System.nanoTime();
    System.err.println("Random Seed = " + seed);
    Random rndm = new Random(seed);
    int payloadSize = rndm.nextInt(KILOBYTES.toBytes(1));
    System.err.println("Payload Size = " + payloadSize);
    
    List<Long> sizes = new ArrayList<Long>();
    List<Long> tableSizes = new ArrayList<Long>();
    
    int terminalKey = 0;
    for (int key = 0; true; key++) {
      int size = selfStealer.size();
      byte[] payload = new byte[payloadSize];
      Arrays.fill(payload, (byte) key);
      selfStealer.put(key, payload);
      sizes.add(Long.valueOf(selfStealer.size()));
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
      sizes.add(Long.valueOf(selfStealer.size()));
      tableSizes.add(postTableSize);
    }

    if (shrinkCount != 0) {
      Map<String, List<? extends Number>> data = new HashMap<String, List<? extends Number>>();
      data.put("actual size", sizes);
      data.put("table size", tableSizes);
      
      JFreeChart chart = ChartFactory.createXYLineChart("Cache Size", "operations", "size", new LongListXYDataset(data), PlotOrientation.VERTICAL, true, false, false);
      File plotOutput = new File("target/TableStealingFromStorageTest.testSelfStealingIsStable.png");
      ChartUtilities.saveChartAsPNG(plotOutput, chart, 640, 480);
      Assert.fail("Expected no shrink events after reaching equilibrium : saw " + shrinkCount + " [plot in " + plotOutput + "]");
    }
  }
  
  static class LongListXYDataset extends AbstractXYDataset {

    private static final long serialVersionUID = 1119808858992366568L;
    
    private final List<String> keys;
    private final List<List<? extends Number>> data;
    
    public LongListXYDataset(Map<String, List<? extends Number>> series) {
      keys = new ArrayList<String>(series.size());
      data = new ArrayList<List<? extends Number>>(series.size());
      
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
      return Integer.valueOf(dataIndex);
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
