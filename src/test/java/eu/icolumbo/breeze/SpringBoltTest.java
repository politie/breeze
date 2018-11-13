package eu.icolumbo.breeze;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.expression.spel.SpelEvaluationException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;


/**
 * Tests {@link SpringBolt}.
 * @author Pascal S. de Kloe
 * @author Jethro Bakker
 */
@RunWith(MockitoJUnitRunner.class)
public class SpringBoltTest {

	@Mock
	Tuple tupleMock;

	@Mock
	OutputCollector outputCollectorMock;

	@Mock
	TopologyContext topologyContextMock;

	@Mock
	OutputFieldsDeclarer outputFieldsDeclarerMock;

	@Mock
	ApplicationContext applicationContextMock;

	Map<String,Object> stormConf = new HashMap<>();

	@Captor
	ArgumentCaptor<Fields> outputFieldsCaptor;


	@Before
	public void init() {
		stormConf.clear();
		stormConf.put("topology.name", "topology");
		doReturn(new TestBean()).when(applicationContextMock).getBean(TestBean.class);
	}

	private void run(SpringBolt subject) {
		subject.setApplicationContext(applicationContextMock);
		subject.prepare(stormConf, topologyContextMock, outputCollectorMock);
		subject.declareOutputFields(outputFieldsDeclarerMock);
		subject.execute(tupleMock);
	}

	/**
	 * Tests simple String in / String out.
	 * - set custom output stream
	 * - default not anchored
	 */
	@Test
	public void pipe() {
		List<Object> expected = asList((Object) "Hello World!");
		doReturn(expected.get(0)).when(tupleMock).getValueByField("in");

		SpringBolt subject = new SpringBolt(TestBean.class, "echo(in)", "out");
		subject.setOutputStreamId("deep");
		run(subject);

		verify(outputFieldsDeclarerMock).declareStream(eq("deep"), outputFieldsCaptor.capture());
		assertEquals(asList("out"), outputFieldsCaptor.getValue().toList());

		InOrder order = inOrder(outputCollectorMock);
		order.verify(outputCollectorMock).emit("deep", tupleMock, expected);
		order.verify(outputCollectorMock).ack(tupleMock);
		order.verifyNoMoreInteractions();
	}

	/**
	 * Tests void in / void out with a integer pass through.
	 * - default output stream
	 * - set not anchored.
	 */
	@Test
	public void sideOperation() {
		when(tupleMock.getValueByField("pass")).thenReturn(9);

		SpringBolt subject = new SpringBolt(TestBean.class, "nop()");
		subject.setPassThroughFields("pass");
		subject.setDoAnchor(false);
		run(subject);

		verify(outputFieldsDeclarerMock).declareStream(eq("default"), outputFieldsCaptor.capture());
		assertEquals(asList("pass"), outputFieldsCaptor.getValue().toList());

		InOrder order = inOrder(outputCollectorMock);
		order.verify(outputCollectorMock).emit("default", asList((Object) 9));
		order.verify(outputCollectorMock).ack(tupleMock);
		order.verifyNoMoreInteractions();
	}

	/**
	 * Tests a null return.
	 */
	@Test
	public void scatterFilter() {
		SpringBolt subject = new SpringBolt(TestBean.class, "nullObject()", "y");
		subject.setScatterOutput(true);
		run(subject);

		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	/**
	 * Tests multiple in / scattered out in combination with a pass though field.
	 */
	@Test
	public void multiplexPassThroughWithScatter() {
		when(tupleMock.getValueByField("a")).thenReturn("first");
		when(tupleMock.getValueByField("b")).thenReturn("second");
		when(tupleMock.getValueByField("c")).thenReturn("routine");

		SpringBolt subject = new SpringBolt(TestBean.class, "array(a, b)", "y");
		subject.setPassThroughFields("c");
		subject.setScatterOutput(true);
		subject.setDoAnchor(false);
		run(subject);

		verify(outputFieldsDeclarerMock).declareStream(eq("default"), outputFieldsCaptor.capture());
		assertEquals(asList("y", "c"), outputFieldsCaptor.getValue().toList());

		InOrder order = inOrder(outputCollectorMock);
		order.verify(outputCollectorMock).emit("default", asList((Object) "first", "routine"));
		order.verify(outputCollectorMock).emit("default", asList((Object) "second", "routine"));
		order.verify(outputCollectorMock).ack(tupleMock);
		order.verifyNoMoreInteractions();
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void passThroughOverlap() {
		SpringBolt subject = new SpringBolt(Object.class, "hashCode()", "hash");
		subject.setPassThroughFields("dupe", "hash");
	}

	@Test
	public void executionException() {
		SpringBolt subject = new SpringBolt(TestBean.class, "clone()");
		run(subject);
		verify(outputCollectorMock, never()).ack(tupleMock);
		verify(outputCollectorMock).fail(tupleMock);
		verify(outputCollectorMock).reportError(isA(CloneNotSupportedException.class));
	}

	@Test
	public void bindingException() {
		SpringBolt subject = new SpringBolt(TestBean.class, "toString()", "c");
		subject.putOutputBinding("c", "broken(666)");
		run(subject);
		verify(outputCollectorMock, never()).ack(tupleMock);
		verify(outputCollectorMock).fail(tupleMock);
		verify(outputCollectorMock).reportError(isA(SpelEvaluationException.class));
	}

	@Test
	public void frameworkError() {
		RuntimeException cause = new RuntimeException("test");
		doThrow(cause).when(tupleMock).getValueByField("oops");

		SpringBolt subject = new SpringBolt(TestBean.class, "echo(oops)");
		try {
			run(subject);
			fail("no exception");
		} catch (RuntimeException e) {
			assertSame(cause, e);
		}

		verifyZeroInteractions(outputCollectorMock);
	}

	@Test
	public void cleanup() {
		new SpringBolt(TestBean.class, "nop()").cleanup();
	}

	/**
	 * Tests the prototyping-context.xml setup with Spring.
	 */
	@Test
	public void prototypeIntegrationRun() {
		stormConf.put("topology.name", "prototyping");

		SpringBolt subject = new SpringBolt(TestBean.class, "hashCode()", "hash");
		subject.prepare(stormConf, topologyContextMock, outputCollectorMock);
		subject.execute(tupleMock);
		subject.execute(tupleMock);

		ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
		verify(outputCollectorMock, times(2)).emit(eq("default"),
				same(tupleMock), captor.capture());
		assertNotEquals(captor.getAllValues().get(0), captor.getAllValues().get(1));
	}

}
