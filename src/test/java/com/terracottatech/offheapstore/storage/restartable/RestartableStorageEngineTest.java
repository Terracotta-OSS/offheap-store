/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.offheapstore.storage.restartable;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.Transaction;
import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.offheapstore.storage.BinaryStorageEngine;
import com.terracottatech.offheapstore.storage.StorageEngine;
import com.terracottatech.offheapstore.storage.listener.RuntimeStorageEngineListener;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import static com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine.*;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 *
 * @author cdennis
 */
public class RestartableStorageEngineTest {
  
  @Test
  public void testThatDelegateWriteFailureIsPropagated() {
    StorageEngine delegate = mock(StorageEngine.class);
    when(delegate.writeMapping(any(), any(), anyInt(), anyInt())).thenReturn(null);
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    assertThat(engine.writeMapping("foo", "bar", 0, 0), nullValue());
  }
  
  @Test
  public void testThatDelegateWriteSuccessIsPropagated() {
    StorageEngine<String, LinkedNode<String>> delegate = mock(StorageEngine.class);
    when(delegate.writeMapping(eq("foo"), argThat(value(is("bar"))), eq(0), eq(0))).thenReturn(42L);
    RestartableStorageEngine<?, String, String, String> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    assertThat(engine.writeMapping("foo", "bar", 0, 0), is(42L));
  }
  
  @Test
  public void testThatDelegateWriteSuccessIsPropagatedToListeners() {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    when(delegate.writeMapping(eq("foo"), argThat(value(is("bar"))), eq(1), eq(2))).thenReturn(42L);
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    when(((BinaryStorageEngine) delegate).readBinaryValue(42L)).thenReturn(ByteBuffer.allocate(25));
    RuntimeStorageEngineListener listener = mock(RuntimeStorageEngineListener.class);
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    engine.registerListener(listener);
    assertThat(engine.writeMapping("foo", "bar", 1, 2), is(42L));
    verify(((BinaryStorageEngine) delegate), times(1)).readBinaryKey(42L);
    verify(listener, only()).written("foo", "bar", ByteBuffer.allocate(1), ByteBuffer.allocate(1), 1, 2, 42L);
  }
  
  @Test
  public void testThatAttachMappingCallsInToRestartability() throws TransactionException {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    when(((BinaryStorageEngine) delegate).readBinaryValue(42L)).thenReturn(ByteBuffer.allocate(25));
    
    Transaction transaction = mockTransaction();
    RestartStore restartability = mockRestartStore(transaction);
    
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", restartability, delegate, true);
    engine.attachedMapping(42L, 0, 0);
    
    verify(restartability, only()).beginTransaction(true);
    verify(transaction, times(1)).put("identifier", encodeKey(ByteBuffer.allocate(1), 0), encodeValue(ByteBuffer.allocate(1), 42L, 0));
    verify(transaction, times(1)).commit();
  }
  
  @Test
  public void testThatTransactionExceptionPropagates() {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    when(((BinaryStorageEngine) delegate).readBinaryValue(42L)).thenReturn(ByteBuffer.allocate(25));
    
    RestartStore restartability = mock(RestartStore.class);
    when(restartability.beginTransaction(true)).thenThrow(TransactionException.class);
    
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", restartability, delegate, true);
    try {
      engine.attachedMapping(42L, 0, 0);
      fail();
    } catch (RuntimeException e) {
      //expected
    }
    
    verify(restartability, times(1)).beginTransaction(true);
    
  }

  @Test
  public void testNonRemovingFreeDoesNotTransact() {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    LinkedNode removal = mock(LinkedNode.class);
    when(removal.getNext()).thenReturn(RestartableStorageEngine.NULL_ENCODING);
    when(removal.getPrevious()).thenReturn(RestartableStorageEngine.NULL_ENCODING);
    when(delegate.readValue(42L)).thenReturn(removal);
    RestartStore restartability = mock(RestartStore.class);
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", restartability, delegate, true);

    engine.freeMapping(42, 0, false);
    
    verifyZeroInteractions(restartability);
  }
  
  @Test
  public void testRemovingFreeDoesTransact() throws TransactionException {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    when(((BinaryStorageEngine) delegate).readBinaryValue(42L)).thenReturn(ByteBuffer.allocate(25));
    LinkedNode mock = mock(LinkedNode.class);
    when(mock.getPrevious()).thenReturn(NULL_ENCODING);
    when(mock.getNext()).thenReturn(NULL_ENCODING);
    when(mock.getValue()).thenReturn(null);
    when(delegate.readValue(42L)).thenReturn(mock);
    
    Transaction transaction = mockTransaction();
    RestartStore restartability = mockRestartStore(transaction);

    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", restartability, delegate, true);

    engine.freeMapping(42, 0, true);
    
    verify(restartability, only()).beginTransaction(true);
    verify(transaction, times(1)).remove("identifier", ByteBuffer.allocate(5));
    verify(transaction, times(1)).commit();
  }
  
  @Test
  public void testRemovingFreeDoesFreeDelegate() throws TransactionException {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    when(((BinaryStorageEngine) delegate).readBinaryValue(42L)).thenReturn(ByteBuffer.allocate(25));
    LinkedNode node = mockLinkedNode(null);
    when(delegate.readValue(42L)).thenReturn(node);
    
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", mockRestartStore(), delegate, true);

    engine.freeMapping(42, 0, true);

    verify(delegate, times(1)).freeMapping(42L, 0, true);
  }
  
  @Test
  public void testNonRemovingFreeDoesFreeDelegate() {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    LinkedNode<?> node = mockLinkedNode(null);
    when(delegate.readValue(42L)).thenReturn(node);
    
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);

    engine.freeMapping(42, 0, false);

    verify(delegate, times(1)).freeMapping(42L, 0, false);
  }
  
  @Test
  public void testRemovingFreeDoesNotifyListeners() throws TransactionException {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1), ByteBuffer.allocate(1));
    when(((BinaryStorageEngine) delegate).readBinaryValue(42L)).thenReturn(ByteBuffer.allocate(25));
    LinkedNode node = mockLinkedNode(null);
    when(delegate.readValue(42L)).thenReturn(node);
    
    RuntimeStorageEngineListener listener = mock(RuntimeStorageEngineListener.class);
    
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", mockRestartStore(), delegate, true);
    engine.registerListener(listener);
    engine.freeMapping(42L, 0, true);

    verify(listener, only()).freed(42L, 0, ByteBuffer.allocate(1), true);
  }
  
  @Test
  public void testNonRemovingFreeDoesNotifyListeners() {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    LinkedNode<?> node = mockLinkedNode(null);
    when(delegate.readValue(42L)).thenReturn(node);
    
    RuntimeStorageEngineListener listener = mock(RuntimeStorageEngineListener.class);
    
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    engine.registerListener(listener);
    engine.freeMapping(42, 0, false);

    verify(listener, only()).freed(42L, 0, ByteBuffer.wrap(new byte[1]), false);
  }
  
  @Test
  public void testValueReadDelegates() {
    StorageEngine delegate = mock(StorageEngine.class);
    LinkedNode<String> node = mockLinkedNode("foo");
    when(delegate.readValue(42L)).thenReturn(node);
    RestartableStorageEngine<?, String, String, String> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);

    assertThat(engine.readValue(42L), is("foo"));
    verify(delegate, only()).readValue(42L);
  }
  
  @Test
  public void testValueEqualsDelegatesCorrectly() {
    StorageEngine delegate = mock(StorageEngine.class);
    LinkedNode<String> node = mockLinkedNode("foo");
    when(delegate.equalsValue(argThat(value(is("foo"))), eq(42L))).thenReturn(true);
    RestartableStorageEngine<?, String, String, String> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);

    assertThat(engine.equalsValue("foo", 42L), is(false));
    assertThat(engine.equalsValue(mockLinkedNode("foo"), 42L), is(true));
    verify(delegate, times(1)).equalsValue("foo", 42L);
    verify(delegate, times(1)).equalsValue(argThat(value(is("foo"))), eq(42L));
  }
  
  @Test
  public void testKeyReadDelegates() {
    StorageEngine delegate = mock(StorageEngine.class);
    when(delegate.readKey(42L, 0)).thenReturn("foo");
    RestartableStorageEngine<?, String, String, String> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);

    assertThat(engine.readKey(42L, 0), is("foo"));
    verify(delegate, only()).readKey(42L, 0);
  }
  
  @Test
  public void testKeyEqualsDelegatesCorrectly() {
    StorageEngine delegate = mock(StorageEngine.class);
    when(delegate.equalsKey("foo", 42L)).thenReturn(true);
    RestartableStorageEngine<?, String, String, String> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);

    assertThat(engine.equalsKey("foo", 42L), is(true));
    assertThat(engine.equalsKey("bar", 42L), is(false));
    verify(delegate, times(1)).equalsKey("foo", 42L);
    verify(delegate, times(1)).equalsKey("bar", 42L);
  }
  
  @Test
  public void testKeyBinaryEqualsDelegatesCorrectly() {
    StorageEngine<String, ?> delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    when(((BinaryStorageEngine) delegate).equalsBinaryKey(ByteBuffer.allocate(1), 42L)).thenReturn(true);
    RestartableStorageEngine<?, String, String, String> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);

    assertThat(engine.equalsBinaryKey(ByteBuffer.allocate(1), 42L), is(true));
    assertThat(engine.equalsBinaryKey(ByteBuffer.allocate(2), 42L), is(false));
    verify((BinaryStorageEngine) delegate, times(1)).equalsBinaryKey(ByteBuffer.allocate(1), 42L);
    verify((BinaryStorageEngine) delegate, times(1)).equalsBinaryKey(ByteBuffer.allocate(2), 42L);
  }
  
  @Test
  public void testClearTransacts() throws TransactionException {
    Transaction<String, String, String> transaction = mockTransaction();
    RestartStore<String, String, String> restartability = mockRestartStore(transaction);
    RestartableStorageEngine<?, String, String, String> engine = new RestartableStorageEngine("identifier", restartability, mock(StorageEngine.class), true);
    
    engine.clear();
    
    verify(restartability, only()).beginTransaction(true);
    verify(transaction, times(1)).delete("identifier");
    verify(transaction, times(1)).commit();
  }
  
  @Test
  public void testClearClearsDelegate() throws TransactionException {
    StorageEngine<String, LinkedNode<String>> delegate = mock(StorageEngine.class);
    RestartableStorageEngine<?, String, String, String> engine = new RestartableStorageEngine("identifier", mockRestartStore(), delegate, true);
    
    engine.clear();

    verify(delegate, only()).clear();
  }
  

  @Test
  public void testClearFiresListener() throws TransactionException {
    RestartableStorageEngine<?, String, String, String> engine = new RestartableStorageEngine("identifier", mockRestartStore(), mock(StorageEngine.class), true);
    RuntimeStorageEngineListener<String, String> listener = mock(RuntimeStorageEngineListener.class);
    engine.registerListener(listener);
    
    engine.clear();

    verify(listener, only()).cleared();
  }

  @Test
  public void testGetAllocatedMemoryDelegates() {
    StorageEngine<?, ?> delegate = mock(StorageEngine.class);
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    
    engine.getAllocatedMemory();
    
    verify(delegate, only()).getAllocatedMemory();
  }
  
  @Test
  public void testGetOccupiedMemoryDelegates() {
    StorageEngine<?, ?> delegate = mock(StorageEngine.class);
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    
    engine.getOccupiedMemory();
    
    verify(delegate, only()).getOccupiedMemory();
  }

  @Test
  public void testInvalidateCacheDelegates() {
    StorageEngine<?, ?> delegate = mock(StorageEngine.class);
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    
    engine.invalidateCache();
    
    verify(delegate, only()).invalidateCache();
  }

  @Test
  public void testBindTriggersBindingToDelegate() {
    StorageEngine<?, ?> delegate = mock(StorageEngine.class);
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    
    engine.bind(mock(Owner.class));

    verify(delegate, only()).bind(engine);
  }
  
  @Test
  public void testDestroyDelegates() {
    StorageEngine<?, ?> delegate = mock(StorageEngine.class);
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    
    engine.destroy();
    
    verify(delegate, only()).destroy();
  }
  
  @Test
  public void testShrinkDelegates() {
    StorageEngine<?, ?> delegate = mock(StorageEngine.class);
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    
    engine.shrink();
    
    verify(delegate, only()).shrink();
  }

  @Test
  public void testSizeDelegatesToOwner() {
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), mock(StorageEngine.class), true);

    Owner owner = mock(Owner.class);
    when(owner.getSize()).thenReturn(1L);
    engine.bind(owner);
    
    assertThat(engine.size(), is(1L));
    
    verify(owner, only()).getSize();
  }

  @Test
  public void testNoLsnsAssigned() {
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), mock(StorageEngine.class), true);
    
    assertThat(engine.lastEncoding(), is(NULL_ENCODING));
    assertThat(engine.firstEncoding(), is(NULL_ENCODING));
  }
  
  @Test
  public void testAssigningToEmpty() {
    StorageEngine delegate = mock(StorageEngine.class);
    LinkedNode<?> node = mockLinkedNode(null);
    when(delegate.readValue(42L)).thenReturn(node);
    
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    engine.assignLsn(42L, 1L);
    
    assertThat(engine.lastEncoding(), is(42L));
    assertThat(engine.firstEncoding(), is(42L));
    
    verify(node, never()).setNext(longThat(not(NULL_ENCODING)));
    verify(node, never()).setPrevious(longThat(not(NULL_ENCODING)));
    verify(node, times(1)).setLsn(1L);
    verify(node, times(1)).flush();
  }
  
  @Test
  public void testFreeingToEmpty() throws TransactionException {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    LinkedNode<?> node = mockLinkedNode(null);
    when(delegate.readValue(42L)).thenReturn(node);
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", mockRestartStore(), delegate, true);
    engine.assignLsn(42L, 1L);
    engine.freeMapping(42L, 0, true);
    
    assertThat(engine.lastEncoding(), is(NULL_ENCODING));
    assertThat(engine.firstEncoding(), is(NULL_ENCODING));
    verify(node, never()).setNext(longThat(not(NULL_ENCODING)));
    verify(node, never()).setPrevious(longThat(not(NULL_ENCODING)));
    verify(node, times(1)).setLsn(1L);
    verify(node, times(2)).flush();
  }
  
  @Test
  public void testAssigningToTail() throws TransactionException {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    LinkedNode<?> node1 = mockLinkedNode("node1");
    when(delegate.readValue(42L)).thenReturn(node1);
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    LinkedNode<?> node2 = mockLinkedNode("node2");
    when(delegate.readValue(43L)).thenReturn(node2);
    when(((BinaryStorageEngine) delegate).readBinaryKey(43L)).thenReturn(ByteBuffer.allocate(1));
    
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", mockRestartStore(), delegate, true);
    
    engine.assignLsn(42L, 1L);
    engine.assignLsn(43L, 2L);
    
    assertThat(engine.firstEncoding(), is(42L));
    assertThat(engine.lastEncoding(), is(43L));
    verify(node1, times(1)).setNext(43L);
    verify(node2, times(1)).setPrevious(42L);
    verify(node1, times(2)).flush();
    verify(node2, times(1)).flush();
  }

  @Test
  public void testFreeingTailNode() throws TransactionException {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    LinkedNode<?> node1 = mockLinkedNode("node1");
    when(delegate.readValue(42L)).thenReturn(node1);
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    LinkedNode<?> node2 = mockLinkedNode("node2");
    when(delegate.readValue(43L)).thenReturn(node2);
    when(((BinaryStorageEngine) delegate).readBinaryKey(43L)).thenReturn(ByteBuffer.allocate(1));
    
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", mockRestartStore(), delegate, true);
    
    engine.assignLsn(42L, 1L);
    engine.assignLsn(43L, 2L);

    engine.freeMapping(43L, 0, true);
    
    assertThat(engine.firstEncoding(), is(42L));
    assertThat(engine.lastEncoding(), is(42L));

    assertThat(node1.getNext(), is(NULL_ENCODING));
    assertThat(node1.getPrevious(), is(NULL_ENCODING));
    
    assertThat(node2.getNext(), is(NULL_ENCODING));
    assertThat(node2.getPrevious(), is(NULL_ENCODING));
    
    verify(node1, never()).setPrevious(longThat(not(NULL_ENCODING)));
    verify(node2, never()).setNext(longThat(not(NULL_ENCODING)));
    verify(node1, times(3)).flush();
    verify(node2, times(2)).flush();
  }


  @Test
  public void testAssigningToHead() throws TransactionException {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    LinkedNode<?> node1 = mockLinkedNode("node1");
    when(delegate.readValue(42L)).thenReturn(node1);
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    LinkedNode<?> node2 = mockLinkedNode("node2");
    when(delegate.readValue(43L)).thenReturn(node2);
    when(((BinaryStorageEngine) delegate).readBinaryKey(43L)).thenReturn(ByteBuffer.allocate(1));
    
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", mockRestartStore(), delegate, true);
    
    engine.assignLsn(42L, 2L);
    engine.assignLsn(43L, 1L);
    
    assertThat(engine.firstEncoding(), is(43L));
    assertThat(engine.lastEncoding(), is(42L));
    
    verify(node1, times(1)).setPrevious(43L);
    verify(node1, never()).setNext(longThat(not(NULL_ENCODING)));
    verify(node2, times(1)).setNext(42L);
    verify(node2, never()).setPrevious(longThat(not(NULL_ENCODING)));
    
    verify(node1, times(2)).flush();
    verify(node2, times(1)).flush();
  }

  @Test
  public void testFreeingFromHead() throws TransactionException {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    LinkedNode<?> node1 = mockLinkedNode("node1");
    when(delegate.readValue(42L)).thenReturn(node1);
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    LinkedNode<?> node2 = mockLinkedNode("node2");
    when(delegate.readValue(43L)).thenReturn(node2);
    when(((BinaryStorageEngine) delegate).readBinaryKey(43L)).thenReturn(ByteBuffer.allocate(1));
    
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", mockRestartStore(), delegate, true);
    
    engine.assignLsn(42L, 1L);
    engine.assignLsn(43L, 2L);

    engine.freeMapping(42L, 0, true);
    
    assertThat(engine.firstEncoding(), is(43L));
    assertThat(engine.lastEncoding(), is(43L));

    assertThat(node1.getNext(), is(NULL_ENCODING));
    assertThat(node1.getPrevious(), is(NULL_ENCODING));
    
    assertThat(node2.getNext(), is(NULL_ENCODING));
    assertThat(node2.getPrevious(), is(NULL_ENCODING));
    
    verify(node1, never()).setPrevious(longThat(not(NULL_ENCODING)));
    verify(node2, never()).setNext(longThat(not(NULL_ENCODING)));
    verify(node1, times(3)).flush();
    verify(node2, times(2)).flush();
  }

  @Test
  public void testAssigningToMiddle() throws TransactionException {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    LinkedNode<?> node1 = mockLinkedNode("node1");
    when(delegate.readValue(42L)).thenReturn(node1);
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    LinkedNode<?> node2 = mockLinkedNode("node2");
    when(delegate.readValue(43L)).thenReturn(node2);
    when(((BinaryStorageEngine) delegate).readBinaryKey(43L)).thenReturn(ByteBuffer.allocate(1));
    LinkedNode<?> node3 = mockLinkedNode("node3");
    when(delegate.readValue(44L)).thenReturn(node3);
    when(((BinaryStorageEngine) delegate).readBinaryKey(44L)).thenReturn(ByteBuffer.allocate(1));
    
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", mockRestartStore(), delegate, true);
    
    engine.assignLsn(42L, 1L);
    engine.assignLsn(43L, 3L);
    engine.assignLsn(44L, 2L);
    
    assertThat(engine.firstEncoding(), is(42L));
    assertThat(engine.lastEncoding(), is(43L));
    
    verify(node2, times(1)).setPrevious(44L);
    verify(node1, times(1)).setNext(44L);
    
    verify(node3, times(1)).setPrevious(42L);
    verify(node3, times(1)).setNext(43L);
    
    verify(node3, times(1)).flush();
  }
  
  @Test
  public void testFreeingFromMiddle() throws TransactionException {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    LinkedNode<?> node1 = mockLinkedNode("node1");
    when(delegate.readValue(42L)).thenReturn(node1);
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    LinkedNode<?> node2 = mockLinkedNode("node2");
    when(delegate.readValue(43L)).thenReturn(node2);
    when(((BinaryStorageEngine) delegate).readBinaryKey(43L)).thenReturn(ByteBuffer.allocate(1));
    LinkedNode<?> node3 = mockLinkedNode("node3");
    when(delegate.readValue(44L)).thenReturn(node3);
    when(((BinaryStorageEngine) delegate).readBinaryKey(44L)).thenReturn(ByteBuffer.allocate(1));
    
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", mockRestartStore(), delegate, true);
    
    engine.assignLsn(42L, 1L);
    engine.assignLsn(43L, 3L);
    engine.assignLsn(44L, 2L);

    engine.freeMapping(44L, 0, true);
    
    assertThat(engine.firstEncoding(), is(42L));
    assertThat(engine.lastEncoding(), is(43L));

    assertThat(node3.getNext(), is(NULL_ENCODING));
    assertThat(node3.getPrevious(), is(NULL_ENCODING));
    verify(node3, times(2)).flush();

    assertThat(node1.getNext(), is(43L));
    assertThat(node2.getPrevious(), is(42L));
    
  }

  @Test
  public void testAcquiringCompactionEntryWhenEmpty() throws TransactionException {
    RestartableStorageEngine engine = new RestartableStorageEngine("identifier", mockRestartStore(), mock(StorageEngine.class), true);
    Owner owner = when(mock(Owner.class).writeLock()).thenReturn(mock(Lock.class)).getMock();
    engine.bind(owner);
    assertThat(engine.acquireCompactionEntry(Long.MAX_VALUE), nullValue());
  }
  
  @Test
  public void testAcquiringCompactionEntryWhenFull() {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    LinkedNode<?> node = mockLinkedNode(null);
    when(delegate.readValue(42L)).thenReturn(node);
    when(((BinaryStorageEngine) delegate).readKeyHash(42L)).thenReturn(10);
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    when(((BinaryStorageEngine) delegate).readBinaryValue(42L)).thenReturn(ByteBuffer.allocate(26));
    
    RestartableStorageEngine<?, String, ByteBuffer, ByteBuffer> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    Lock ownerLock = mock(Lock.class);
    Owner owner = when(mock(Owner.class).writeLock()).thenReturn(ownerLock).getMock();
    engine.bind(owner);
    
    engine.assignLsn(42L, 1L);
    
    ObjectManagerEntry<String, ByteBuffer, ByteBuffer> entry = engine.acquireCompactionEntry(Long.MAX_VALUE);
    assertThat(entry.getId(), is("identifier"));
    assertThat(decodeKey(entry.getKey()), is(ByteBuffer.allocate(1)));
    assertThat(extractHashcode(entry.getKey()), is(10));
    assertThat(decodeValue(entry.getValue()), is(ByteBuffer.allocate(2)));
    assertThat(extractEncoding(entry.getValue()), is(42L));
    assertThat(extractMetadata(entry.getValue()), is(0));
    assertThat(entry.getLsn(), is(1L));

    verify(ownerLock, only()).lock();
  }
  
  @Test
  public void testAcquiringCompactionEntryAboveCeiling() {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    LinkedNode<?> node = mockLinkedNode(null);
    when(delegate.readValue(42L)).thenReturn(node);
    when(((BinaryStorageEngine) delegate).readKeyHash(42L)).thenReturn(10);
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    when(((BinaryStorageEngine) delegate).readBinaryValue(42L)).thenReturn(ByteBuffer.allocate(26));
    
    RestartableStorageEngine<?, String, ByteBuffer, ByteBuffer> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    Owner owner = when(mock(Owner.class).writeLock()).thenReturn(mock(Lock.class)).getMock();
    engine.bind(owner);
    
    engine.assignLsn(42L, 1L);
    
    assertThat(engine.acquireCompactionEntry(1), nullValue());
  }
  
  @Test
  public void testUpdateLsnWithInvalidEntry() {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    LinkedNode<?> node = mockLinkedNode(null);
    when(delegate.readValue(42L)).thenReturn(node);
    when(((BinaryStorageEngine) delegate).readKeyHash(42L)).thenReturn(10);
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    when(((BinaryStorageEngine) delegate).readBinaryValue(42L)).thenReturn(ByteBuffer.allocate(26));
    
    RestartableStorageEngine<?, String, ByteBuffer, ByteBuffer> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    Owner owner = when(mock(Owner.class).writeLock()).thenReturn(mock(Lock.class)).getMock();
    engine.bind(owner);
    
    engine.assignLsn(42L, 1L);
    
    ObjectManagerEntry<String, ByteBuffer, ByteBuffer> entry = engine.acquireCompactionEntry(Long.MAX_VALUE);
    
    try {
      engine.updateLsn(10, mock(ObjectManagerEntry.class), 2L);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }
  
  @Test
  public void testUpdateLsnWithValidEntry() {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    LinkedNode<?> node = mockLinkedNode(null);
    when(delegate.readValue(42L)).thenReturn(node);
    when(((BinaryStorageEngine) delegate).readKeyHash(42L)).thenReturn(10);
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    when(((BinaryStorageEngine) delegate).readBinaryValue(42L)).thenReturn(ByteBuffer.allocate(26));
    
    RestartableStorageEngine<?, String, ByteBuffer, ByteBuffer> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    Owner owner = when(mock(Owner.class).writeLock()).thenReturn(mock(Lock.class)).getMock();
    engine.bind(owner);
    
    engine.assignLsn(42L, 1L);
    
    ObjectManagerEntry<String, ByteBuffer, ByteBuffer> entry = engine.acquireCompactionEntry(Long.MAX_VALUE);
    
    engine.updateLsn(10, entry, 2L);
    
    verify(node, times(1)).setLsn(2L);
  }
  
  @Test
  public void testReleaseCompactionEntryFailsOnNull() {
    RestartableStorageEngine<?, String, ByteBuffer, ByteBuffer> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), mock(StorageEngine.class), true);
    try {
      engine.releaseCompactionEntry(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }
  
  @Test
  public void testReleaseCompactionEntryFailsOnMismatch() {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    LinkedNode<?> node = mockLinkedNode(null);
    when(delegate.readValue(42L)).thenReturn(node);
    when(((BinaryStorageEngine) delegate).readKeyHash(42L)).thenReturn(10);
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    when(((BinaryStorageEngine) delegate).readBinaryValue(42L)).thenReturn(ByteBuffer.allocate(26));
    
    RestartableStorageEngine<?, String, ByteBuffer, ByteBuffer> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    Owner owner = when(mock(Owner.class).writeLock()).thenReturn(mock(Lock.class)).getMock();
    engine.bind(owner);
    
    engine.assignLsn(42L, 1L);
    
    ObjectManagerEntry<String, ByteBuffer, ByteBuffer> entry = engine.acquireCompactionEntry(Long.MAX_VALUE);
    
    try {
      engine.releaseCompactionEntry(mock(ObjectManagerEntry.class));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }
  
  @Test
  public void testReleaseCompactionEntryUnlocks() {
    StorageEngine delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    LinkedNode<?> node = mockLinkedNode(null);
    when(delegate.readValue(42L)).thenReturn(node);
    when(((BinaryStorageEngine) delegate).readKeyHash(42L)).thenReturn(10);
    when(((BinaryStorageEngine) delegate).readBinaryKey(42L)).thenReturn(ByteBuffer.allocate(1));
    when(((BinaryStorageEngine) delegate).readBinaryValue(42L)).thenReturn(ByteBuffer.allocate(26));
    
    RestartableStorageEngine<?, String, ByteBuffer, ByteBuffer> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    Lock ownerLock = mock(Lock.class);
    Owner owner = when(mock(Owner.class).writeLock()).thenReturn(ownerLock).getMock();
    engine.bind(owner);
    
    engine.assignLsn(42L, 1L);
    
    ObjectManagerEntry<String, ByteBuffer, ByteBuffer> entry = engine.acquireCompactionEntry(Long.MAX_VALUE);
    
    engine.releaseCompactionEntry(entry);
    
    verify(ownerLock, times(1)).lock();
    verify(ownerLock, times(1)).unlock();
  }
  
  @Test
  public void testReplayPutInstallsToOwner() {
    StorageEngine delegate = mock(StorageEngine.class);
    when(delegate.readValue(42L)).thenReturn(mockLinkedNode("foo"));
    Owner owner = mock(Owner.class);
    when(owner.writeLock()).thenReturn(mock(Lock.class));
    when(owner.installMappingForHashAndEncoding(42, ByteBuffer.allocate(1), ByteBuffer.allocate(2), 0)).thenReturn(42L);
    RestartableStorageEngine<?, String, ByteBuffer, ByteBuffer> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    engine.bind(owner);
    
    engine.replayPut(42, encodeKey(ByteBuffer.allocate(1), 42), encodeValue(ByteBuffer.allocate(2), 0, 0), 1);
    
    verify(owner, times(1)).installMappingForHashAndEncoding(anyInt(), any(ByteBuffer.class), any(ByteBuffer.class), anyInt());
  }
  
  @Test
  public void testReplayPutToEmptyLinksProperly() {
    StorageEngine delegate = mock(StorageEngine.class);
    LinkedNode node = mockLinkedNode("foo");
    when(delegate.readValue(42L)).thenReturn(node);
    Owner owner = mock(Owner.class);
    when(owner.writeLock()).thenReturn(mock(Lock.class));
    when(owner.installMappingForHashAndEncoding(42, ByteBuffer.allocate(1), ByteBuffer.allocate(2), 0)).thenReturn(42L);
    RestartableStorageEngine<?, String, ByteBuffer, ByteBuffer> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    engine.bind(owner);
    
    engine.replayPut(42, encodeKey(ByteBuffer.allocate(1), 42), encodeValue(ByteBuffer.allocate(2), 0, 0), 1);

    assertThat(engine.firstEncoding(), is(42L));
    assertThat(engine.lastEncoding(), is(42L));
    assertThat(node.getPrevious(), is(NULL_ENCODING));
    assertThat(node.getNext(), is(NULL_ENCODING));
  }
  
  @Test
  public void testReplayPutToHeadLinksProperly() {
    StorageEngine delegate = mock(StorageEngine.class);
    LinkedNode node1 = mockLinkedNode("foo");
    when(delegate.readValue(42L)).thenReturn(node1);
    LinkedNode node2 = mockLinkedNode("bar");
    when(delegate.readValue(43L)).thenReturn(node2);
    
    Owner owner = mock(Owner.class);
    when(owner.writeLock()).thenReturn(mock(Lock.class));
    when(owner.installMappingForHashAndEncoding(42, ByteBuffer.allocate(1), ByteBuffer.allocate(2), 0)).thenReturn(42L);
    when(owner.installMappingForHashAndEncoding(43, ByteBuffer.allocate(1), ByteBuffer.allocate(2), 0)).thenReturn(43L);
    RestartableStorageEngine<?, String, ByteBuffer, ByteBuffer> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    engine.bind(owner);
    
    engine.replayPut(42, encodeKey(ByteBuffer.allocate(1), 42), encodeValue(ByteBuffer.allocate(2), 0, 0), 2);
    engine.replayPut(43, encodeKey(ByteBuffer.allocate(1), 43), encodeValue(ByteBuffer.allocate(2), 0, 0), 1);

    assertThat(engine.firstEncoding(), is(43L));
    assertThat(engine.lastEncoding(), is(42L));
    assertThat(node1.getPrevious(), is(43L));
    assertThat(node2.getNext(), is(42L));
    assertThat(node2.getPrevious(), is(NULL_ENCODING));
    verify(node2, times(1)).flush();
  }
  
  @Test
  public void testReplayPutToTailLinksProperly() {
    StorageEngine delegate = mock(StorageEngine.class);
    LinkedNode node1 = mockLinkedNode("foo");
    when(delegate.readValue(42L)).thenReturn(node1);
    LinkedNode node2 = mockLinkedNode("bar");
    when(delegate.readValue(43L)).thenReturn(node2);
    
    Owner owner = mock(Owner.class);
    when(owner.writeLock()).thenReturn(mock(Lock.class));
    when(owner.installMappingForHashAndEncoding(42, ByteBuffer.allocate(1), ByteBuffer.allocate(2), 0)).thenReturn(42L);
    when(owner.installMappingForHashAndEncoding(43, ByteBuffer.allocate(1), ByteBuffer.allocate(2), 0)).thenReturn(43L);
    RestartableStorageEngine<?, String, ByteBuffer, ByteBuffer> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    engine.bind(owner);
    
    engine.replayPut(42, encodeKey(ByteBuffer.allocate(1), 42), encodeValue(ByteBuffer.allocate(2), 0, 0), 1);
    engine.replayPut(43, encodeKey(ByteBuffer.allocate(1), 43), encodeValue(ByteBuffer.allocate(2), 0, 0), 2);

    assertThat(engine.firstEncoding(), is(42L));
    assertThat(engine.lastEncoding(), is(43L));
    assertThat(node1.getNext(), is(43L));
    assertThat(node2.getPrevious(), is(42L));
    assertThat(node2.getNext(), is(NULL_ENCODING));
    verify(node2, times(1)).flush();
  }
  
  @Test
  public void testReplayPutToMiddleLinksProperly() {
    StorageEngine delegate = mock(StorageEngine.class);
    LinkedNode node1 = mockLinkedNode("foo");
    when(delegate.readValue(42L)).thenReturn(node1);
    LinkedNode node2 = mockLinkedNode("bar");
    when(delegate.readValue(43L)).thenReturn(node2);
    LinkedNode node3 = mockLinkedNode("baz");
    when(delegate.readValue(44L)).thenReturn(node3);
    
    Owner owner = mock(Owner.class);
    when(owner.writeLock()).thenReturn(mock(Lock.class));
    when(owner.installMappingForHashAndEncoding(42, ByteBuffer.allocate(1), ByteBuffer.allocate(2), 0)).thenReturn(42L);
    when(owner.installMappingForHashAndEncoding(43, ByteBuffer.allocate(1), ByteBuffer.allocate(2), 0)).thenReturn(43L);
    when(owner.installMappingForHashAndEncoding(44, ByteBuffer.allocate(1), ByteBuffer.allocate(2), 0)).thenReturn(44L);
    RestartableStorageEngine<?, String, ByteBuffer, ByteBuffer> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    engine.bind(owner);
    
    engine.replayPut(42, encodeKey(ByteBuffer.allocate(1), 42), encodeValue(ByteBuffer.allocate(2), 0, 0), 1);
    engine.replayPut(43, encodeKey(ByteBuffer.allocate(1), 43), encodeValue(ByteBuffer.allocate(2), 0, 0), 3);
    engine.replayPut(44, encodeKey(ByteBuffer.allocate(1), 44), encodeValue(ByteBuffer.allocate(2), 0, 0), 2);

    assertThat(engine.firstEncoding(), is(42L));
    assertThat(engine.lastEncoding(), is(43L));
    assertThat(node1.getNext(), is(44L));
    assertThat(node3.getPrevious(), is(42L));
    assertThat(node3.getNext(), is(43L));
    assertThat(node2.getPrevious(), is(44L));
    verify(node3, times(1)).flush();
  }
  
  @Test
  public void testWriteBinaryMappingDelegatesCorrectly() {
    StorageEngine<?, ?> delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);

    engine.writeBinaryMapping(ByteBuffer.allocate(1), ByteBuffer.allocate(2), 42, 1);
    
    verify((BinaryStorageEngine) delegate, only()).writeBinaryMapping(new ByteBuffer[] {ByteBuffer.allocate(1)}, new ByteBuffer[] {LinkedNodePortability.emptyHeader(), ByteBuffer.allocate(2)}, 42, 1);
  }
  
  @Test
  public void testWriteBinaryMappingArrayDelegatesCorrectly() {
    StorageEngine<?, ?> delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);

    engine.writeBinaryMapping(new ByteBuffer[] {ByteBuffer.allocate(1)}, new ByteBuffer[] {ByteBuffer.allocate(2)}, 42, 1);
    
    verify((BinaryStorageEngine) delegate, only()).writeBinaryMapping(new ByteBuffer[] {ByteBuffer.allocate(1)}, new ByteBuffer[] {LinkedNodePortability.emptyHeader(), ByteBuffer.allocate(2)}, 42, 1);
  }

  @Test
  public void testReadKeyHashDelegates() {
    StorageEngine<?, ?> delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);

    ((BinaryStorageEngine) engine).readKeyHash(42L);
    
    verify((BinaryStorageEngine) delegate, only()).readKeyHash(42L);
  }

  @Test
  public void testReadBinaryKeyDelegates() {
    StorageEngine<?, ?> delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);

    ((BinaryStorageEngine) engine).readBinaryKey(42L);
    
    verify((BinaryStorageEngine) delegate, only()).readBinaryKey(42L);
  }

  @Test
  public void testReadBinaryValueDelegatesCorrectly() {
    StorageEngine<?, ?> delegate = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    when(((BinaryStorageEngine) delegate).readBinaryValue(42L)).thenReturn(ByteBuffer.allocate(25));
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    
    assertThat(((BinaryStorageEngine) engine).readBinaryValue(42L), is(ByteBuffer.allocate(1)));
  }
  
  @Test
  public void testSizeInBytesDelegates() {
    StorageEngine<?, ?> delegate = mock(StorageEngine.class);
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);

    engine.sizeInBytes();
    
    verify(delegate, only()).getOccupiedMemory();
  }
  
  @Test
  public void testGetEncodingDelegates() {
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), mock(StorageEngine.class), true);
    Owner owner = mock(Owner.class);
    
    engine.bind(owner);

    engine.getEncodingForHashAndBinary(42, ByteBuffer.allocate(1));
    
    verify(owner, only()).getEncodingForHashAndBinary(42, ByteBuffer.allocate(1));
  }
  
  @Test
  public void testGetSizeDelegates() {
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), mock(StorageEngine.class), true);
    Owner owner = mock(Owner.class);
    
    engine.bind(owner);

    engine.getSize();
    
    verify(owner, only()).getSize();
  }
  
  @Test
  public void testInstallMappingDelegates() {
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), mock(StorageEngine.class), true);
    Owner owner = mock(Owner.class);
    
    engine.bind(owner);

    engine.installMappingForHashAndEncoding(42, ByteBuffer.allocate(1), ByteBuffer.allocate(2), 2);
    
    verify(owner, only()).installMappingForHashAndEncoding(42, ByteBuffer.allocate(1), ByteBuffer.allocate(2), 2);
  }

  @Test
  public void testEncodingSetDelegates() {
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), mock(StorageEngine.class), true);
    Owner owner = mock(Owner.class);
    
    engine.bind(owner);

    engine.encodingSet();
    
    verify(owner, only()).encodingSet();
  }
  
  @Test
  public void testEncodingUpdateFailsIfOwnerFails() {
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), mock(StorageEngine.class), true);
    Owner owner = mock(Owner.class);
    when(owner.updateEncoding(anyInt(), anyLong(), anyLong(), anyLong())).thenReturn(false);
    engine.bind(owner);
    
    assertThat(engine.updateEncoding(42, 43L, 44l, ~0), is(false));
  }

  @Test
  public void testLinksAreUpdatedIfOwnerPasses() {
    StorageEngine delegate = mock(StorageEngine.class);
    LinkedNode previousNode = mockLinkedNode("previous", NULL_ENCODING, 42L);
    when(delegate.readValue(1L)).thenReturn(previousNode);
    
    LinkedNode node = mockLinkedNode("node", 1L, 2L);
    when(delegate.readValue(84L)).thenReturn(node);
    LinkedNode nextNode = mockLinkedNode("next", 42L, NULL_ENCODING);
    when(delegate.readValue(2L)).thenReturn(nextNode);
    
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), delegate, true);
    Owner owner = mock(Owner.class);
    when(owner.updateEncoding(anyInt(), anyLong(), anyLong(), anyLong())).thenReturn(true);
    engine.bind(owner);
    
    assertThat(engine.updateEncoding(10, 42L, 84l, ~0), is(true));
    
    assertThat(previousNode.getNext(), is(84L));
    assertThat(nextNode.getPrevious(), is(84L));
  }

  @Test
  public void testGetSlotDelegates() {
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), mock(StorageEngine.class), true);
    Owner owner = mock(Owner.class);
    
    engine.bind(owner);

    engine.getSlotForHashAndEncoding(10, 42L, ~0L);
    
    verify(owner, only()).getSlotForHashAndEncoding(10, 42L, ~0L);
  }
  
  @Test
  public void testEvictDelegates() {
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), mock(StorageEngine.class), true);
    Owner owner = mock(Owner.class);
    
    engine.bind(owner);

    engine.evict(42, true);
    
    verify(owner, only()).evict(42, true);
  }
  
  @Test
  public void testIsThiefDelegates() {
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), mock(StorageEngine.class), true);
    Owner owner = mock(Owner.class);
    
    engine.bind(owner);

    engine.isThiefForTableAllocations();
    
    verify(owner, only()).isThiefForTableAllocations();
  }

  @Test
  public void testReadLockDelegates() {
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), mock(StorageEngine.class), true);
    Owner owner = mock(Owner.class);
    
    engine.bind(owner);

    engine.readLock();
    
    verify(owner, only()).readLock();
  }
  
  @Test
  public void testWriteLockDelegates() {
    RestartableStorageEngine<?, ?, ?, ?> engine = new RestartableStorageEngine("identifier", mock(RestartStore.class), mock(StorageEngine.class), true);
    Owner owner = mock(Owner.class);
    
    engine.bind(owner);

    engine.writeLock();
    
    verify(owner, only()).writeLock();
  }
  
  private static <I, K, V> RestartStore<I, K, V> mockRestartStore() throws TransactionException {
    return mockRestartStore(mockTransaction());
  }
  
  private static <I, K, V> RestartStore<I, K, V> mockRestartStore(Transaction txn) throws TransactionException {
    RestartStore<I, K, V> mock = mock(RestartStore.class);
    when(mock.beginTransaction(anyBoolean())).thenReturn(txn);
    return mock;
  }
  
  private static <I, K, V> Transaction<I, K, V> mockTransaction() throws TransactionException {
    Transaction<I, K, V> mock = mock(Transaction.class);
    when(mock.delete((I) any())).thenReturn(mock);
    when(mock.remove((I) any(), (K) any())).thenReturn(mock);
    when(mock.put((I) any(), (K) any(), (V) any())).thenReturn(mock);
    return mock;
  }
  
  private static <T> LinkedNode<T> mockLinkedNode(T value) {
    return mockLinkedNode(value, NULL_ENCODING, NULL_ENCODING);
  }
  
  private static <T> LinkedNode<T> mockLinkedNode(T value, long previous, long next) {
    LinkedNode<T> real = new StubLinkedNode<T>(value, 0);
    real.setPrevious(previous);
    real.setNext(next);
    return spy(real);
  }
  
  private static <T> Matcher<LinkedNode<T>> value(final Matcher<T> matcher) {
    return new TypeSafeMatcher<LinkedNode<T>>() {

      @Override
      protected boolean matchesSafely(LinkedNode<T> item) {
        return matcher.matches(item.getValue());
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("has value").appendDescriptionOf(matcher);
      }
    };
  }
  
  private static class StubLinkedNode<T> implements LinkedNode<T> {

    private final T value;
    private final int metadata;
    
    private long lsn;
    private long previous;
    private long next;

    public StubLinkedNode(T value, int metadata) {
      this.value = value;
      this.metadata = metadata;
    }
    
    @Override
    public long getLsn() {
      return lsn;
    }

    @Override
    public void setLsn(long lsn) {
      this.lsn = lsn;
    }

    @Override
    public long getNext() {
      return next;
    }

    @Override
    public long getPrevious() {
      return previous;
    }

    @Override
    public void setNext(long encoding) {
      next = encoding;
    }

    @Override
    public void setPrevious(long encoding) {
      previous = encoding;
    }

    @Override
    public T getValue() {
      return value;
    }

    @Override
    public void flush() {
      //no-op
    }

    @Override
    public int getMetadata() {
      return metadata;
    }
    
  }
}
