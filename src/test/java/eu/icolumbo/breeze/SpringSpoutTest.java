package eu.icolumbo.breeze;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.AcceptPendingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;


/**
 * Tests {@link SpringSpout}.
 * @author Jethro Bakker
 * @author Pascal S. de Kloe
 */
@RunWith(MockitoJUnitRunner.class)
public class SpringSpoutTest {

	@Mock
	private SpoutOutputCollector collectorMock;

	@Mock
	private TopologyContext contextMock;

	@Mock
	ApplicationContext applicationContextMock;

	Map<String,Object> stormConf = new HashMap<>();

	@Before
	public void setup() {
		stormConf.clear();
		stormConf.put("topology.name", "simple");
	}

	@Test
	public void happyFlow() throws Exception {
		SpringSpout subject = new SpringSpout(TestBean.class, "ping()", "out");
		subject.setOutputStreamId("ether");

		subject.open(stormConf, contextMock, collectorMock);
		subject.nextTuple();

		verify(collectorMock).emit("ether", asList((Object) "ping"));
	}

	/**
	 * Tests the {@link SpringSpout#setAckSignature(String) ack signature} effect
	 * with {@link SpringSpout#setScatterOutput(boolean) record chunks}.
	 */
	@Test
	public void ackTransaction() throws Exception {
		TestBean.Data record1 = new TestBean.Data();
		record1.setId(0);
		record1.setMessage("ding");
		TestBean.Data record2 = new TestBean.Data();
		record2.setId(1);
		record2.setMessage("dong");

		List<Object> bean = new ArrayList<>();
		bean.add(record1);
		bean.add(record2);
		doReturn(bean).when(applicationContextMock).getBean(bean.getClass());

		SpringSpout subject = new SpringSpout(bean.getClass(), "toArray()", "g");
		subject.setScatterOutput(true);
		subject.setAckSignature("set(id, message)");

		subject.setApplicationContext(applicationContextMock);
		subject.open(stormConf, contextMock, collectorMock);
		subject.nextTuple();

		ArgumentCaptor<Object> messageIdCaptor = ArgumentCaptor.forClass(Object.class);
		verify(collectorMock).emit(eq("default"), eq(bean.subList(0, 1)), messageIdCaptor.capture());
		verify(collectorMock).emit(eq("default"), eq(bean.subList(1, 2)), messageIdCaptor.capture());
		verifyNoMoreInteractions(collectorMock);

		subject.ack(messageIdCaptor.getAllValues().get(0));
		subject.ack(messageIdCaptor.getAllValues().get(1));
		assertEquals(asList((Object) "ding", "dong"), bean);
	}

	/**
	 * Tests the {@link SpringSpout#setFailSignature(String) fail signature} effect
	 * on a {@link SpringSpout#setOutputStreamId(String) custom stream ID} with collection fields.
	 */
	@Test
	public void failTransaction() throws Exception {
		List<Object> bean = new ArrayList<>();
		bean.add("dang");
		doReturn(bean).when(applicationContextMock).getBean(bean.getClass());

		SpringSpout subject = new SpringSpout(bean.getClass(), "clone()", "x");
		subject.setFailSignature("clear()");
		subject.setOutputStreamId("universe");

		subject.setApplicationContext(applicationContextMock);
		subject.open(stormConf, contextMock, collectorMock);
		subject.nextTuple();

		ArgumentCaptor<Object> messageIdCaptor = ArgumentCaptor.forClass(Object.class);
		verify(collectorMock).emit(eq("universe"), eq(asList((Object) bean)), messageIdCaptor.capture());

		subject.fail(messageIdCaptor.getValue());
		assertEquals(Collections.emptyList(), bean);
	}

	@Test
	public void operationException() throws Exception {
		SpringSpout subject = new SpringSpout(TestBean.class, "clone()", "copy");

		subject.open(stormConf, contextMock, collectorMock);
		subject.nextTuple();

		verify(collectorMock).reportError(isA(CloneNotSupportedException.class));
		verifyNoMoreInteractions(collectorMock);
	}

	@Test
	public void bindingException() throws Exception {
		SpringSpout subject = new SpringSpout(TestBean.class, "toString()", "c");
		subject.putOutputBinding("c", "charAt(666)");

		subject.open(stormConf, contextMock, collectorMock);
		subject.nextTuple();

		verify(collectorMock).reportError(isA(StringIndexOutOfBoundsException.class));
		verifyNoMoreInteractions(collectorMock);
	}

	@Test
	public void operationDelay() throws Exception {
		long acceptDelay = 20;
		long ioDelay = 80;

		BufferedReader bean = mock(BufferedReader.class);
		doReturn(bean).when(applicationContextMock).getBean(bean.getClass());
		when(bean.readLine()).thenThrow(new AcceptPendingException(), new EOFException());

		SpringSpout subject = new SpringSpout(bean.getClass(), "readLine()", "line");
		subject.setApplicationContext(applicationContextMock);
		subject.putDelayException(AcceptPendingException.class, acceptDelay);
		subject.putDelayException(IOException.class, ioDelay);
		subject.open(stormConf, contextMock, collectorMock);

		long start = System.currentTimeMillis();
		subject.nextTuple();
		long acceptEnd = System.currentTimeMillis();
		subject.nextTuple();
		long end = System.currentTimeMillis();
		verifyZeroInteractions(collectorMock);
		assertTrue("accept delay", acceptDelay <= acceptEnd - start);
		assertTrue("io delay", acceptDelay <= end - acceptEnd);
	}

	@Test
	public void nop() {
		SpringSpout subject = new SpringSpout(Object.class, "nop()");

		subject.close();
		subject.activate();
		subject.deactivate();
		subject.ack(null);
		subject.fail(null);
		verifyZeroInteractions(contextMock);
		verifyZeroInteractions(collectorMock);
	}

	@Test
	public void ackSignatureMismatch() {
		SpringSpout subject = new SpringSpout(TestBean.class, "ping()", "out");
		subject.setAckSignature("doesNotExist()");
		try {
			subject.open(stormConf, contextMock, collectorMock);
			fail("no exception");
		} catch (IllegalStateException e) {
			assertEquals("Unusable transaction signature", e.getMessage());
		}
	}

	@Test
	public void ackException() {
		SpringSpout subject = new SpringSpout(TestBean.class, "toString()", "s");
		subject.setAckSignature("clone()");
		subject.open(stormConf, contextMock, collectorMock);
		subject.ack(new TransactionContext());
	}

	@Test
	public void failException() {
		SpringSpout subject = new SpringSpout(TestBean.class, "toString()", "s");
		subject.setFailSignature("clone()");
		subject.open(stormConf, contextMock, collectorMock);
		subject.fail(new TransactionContext());
	}

}
