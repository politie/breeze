package eu.icolumbo.breeze;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


/**
 * Tests {@link SpringComponent}.
 * @author Pascal S. de Kloe
 * @author Jethro Bakker
 */
@RunWith(MockitoJUnitRunner.class)
public class SpringComponentTest {

	@Mock
	TopologyContext topologyContextMock;

	@Mock
	ApplicationContext applicationContextMock;

	Map stormConf = new HashMap();

	@Before
	public void init() {
		stormConf.clear();
		doReturn("test-topology").when(topologyContextMock).getStormId();
	}

	@Test
	public void output() {
		SpringComponent subject = new SpringComponent(Collection.class, "toArray()", "a", "b", "c") {};
		subject.setOutputStreamId("elements");
		assertArrayEquals(new String[]{"a", "b", "c"}, subject.getOutputFields());

		OutputFieldsDeclarer mock = mock(OutputFieldsDeclarer.class);
		subject.declareOutputFields(mock);

		ArgumentCaptor<Fields> captor = ArgumentCaptor.forClass(Fields.class);
		verify(mock).declareStream(eq("elements"), captor.capture());
		assertEquals(0, captor.getValue().fieldIndex("a"));
		assertEquals(2, captor.getValue().fieldIndex("c"));
	}

	@Test
	public void componentConfiguration() {
		SpringComponent subject = new SpringComponent(Collection.class, "clear()") {};
		assertNull(subject.getComponentConfiguration());
	}

	@Test
	public void propertyInvocation() {
		try {
			new SpringComponent(Collection.class, "class") {};
			fail("no exception");
		} catch (IllegalArgumentException e) {
			assertEquals("Malformed method signature: class", e.getMessage());
		}
	}

	@Test
	public void nestedInvocation() {
		try {
			new SpringComponent(Collection.class, "iterator().next()") {};
			fail("no exception");
		} catch (IllegalArgumentException e) {
			assertEquals("Malformed method signature: iterator().next()", e.getMessage());
		}
	}

	@Test
	public void nonexistentInvocation() {
		try {
			new SpringComponent(Collection.class, "fast()") {
			}.init(stormConf, topologyContextMock);
			fail("no exception");
		} catch (IllegalStateException e) {
			assertEquals("Can't use configured bean method", e.getMessage());
			assertNotNull(e.getCause());
		}
	}

	@Test(expected=IllegalArgumentException.class)
	public void argumentMismatch() throws Exception {
		TestBean bean = new TestBean();
		bean.setGreeting("Hello");
		doReturn(bean).when(applicationContextMock).getBean(bean.getClass());

		SpringComponent subject = new SpringComponent(bean.getClass(), "array(a, b)") {};
		subject.setApplicationContext(applicationContextMock);
		subject.init(stormConf, topologyContextMock);

		subject.invoke(subject.method, null, 8);
	}

	@Test
	public void multipleOutputFieldsBean() throws Exception {
		TestBean bean = new TestBean();
		bean.setGreeting("Hello");
		doReturn(bean).when(applicationContextMock).getBean(bean.getClass());

		SpringComponent subject = new SpringComponent(bean.getClass(), "greet(n)", "id", "message", "unknown") {};
		subject.setApplicationContext(applicationContextMock);
		subject.init(stormConf, topologyContextMock);

		Object[] entries = subject.invoke(subject.method, 8);
		Values[] result = new Values[entries.length];
		for(int i=0; i<entries.length; i++) {
			result[i] = subject.getMapping(entries[i], subject.getOutputFields());
		}

		Values[] expected = {new Values(8, "Hello 8", null)};
		assertArrayEquals(expected, result);
	}

	@Test
	public void multipleOutputFieldsBeanBroken() throws Exception {
		TestBean bean = new TestBean();
		bean.setGreeting("Hello");
		doReturn(bean).when(applicationContextMock).getBean(bean.getClass());

		SpringComponent subject = new SpringComponent(bean.getClass(), "greet(n)", "setterOnly", "privateGetter") {};
		subject.setApplicationContext(applicationContextMock);
		subject.init(stormConf, topologyContextMock);

		Object[] entries = subject.invoke(subject.method, 8);
		Values[] result = new Values[entries.length];
		for(int i=0; i<entries.length; i++) {
			result[i] = subject.getMapping(entries[i], subject.getOutputFields());
		}

		Values[] expected = {new Values(null, null)};
		assertArrayEquals(expected, result);
	}
}
