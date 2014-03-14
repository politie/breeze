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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.util.Arrays.asList;
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
	public void propertySignature() {
		try {
			new SpringComponent(Collection.class, "class") {};
			fail("no exception");
		} catch (IllegalArgumentException e) {
			assertEquals("Malformed method signature: class", e.getMessage());
		}
	}

	@Test
	public void nestedSignature() {
		try {
			new SpringComponent(Collection.class, "iterator().next()") {};
			fail("no exception");
		} catch (IllegalArgumentException e) {
			assertEquals("Malformed method signature: iterator().next()", e.getMessage());
		}
	}

	@Test
	public void nonexistentSignature() {
		try {
			new SpringComponent(Collection.class, "fast()") {
			}.init(stormConf, topologyContextMock);
			fail("no exception");
		} catch (IllegalStateException e) {
			assertEquals("Unusable input signature", e.getMessage());
			assertNotNull(e.getCause());
		}
	}

	@Test(expected=IllegalArgumentException.class)
	public void argumentMismatch() throws Exception {
		TestBean bean = new TestBean();
		doReturn(bean).when(applicationContextMock).getBean(bean.getClass());

		SpringComponent subject = new SpringComponent(bean.getClass(), "array(a, b)") {};
		subject.setApplicationContext(applicationContextMock);
		subject.init(stormConf, topologyContextMock);

		subject.invoke(new Object[] {null, 8});
	}

	@Test
	public void outputRegistration() {
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
	public void arrayReturns() throws Exception {
		String[] data = {"a", "b"};

		Collection<String> bean = asList(data);
		doReturn(bean).when(applicationContextMock).getBean(bean.getClass());

		SpringComponent subject = new SpringComponent(bean.getClass(), "toArray()", "x") {};
		subject.setApplicationContext(applicationContextMock);
		subject.init(stormConf, topologyContextMock);

		assertArrayEquals("return", new Object[]{data}, subject.invoke(new Object[0]));
		subject.setScatterOutput(true);
		assertArrayEquals("scattered return", data, subject.invoke(new Object[0]));
	}

	@Test
	public void collectionReturns() throws Exception {
		Collection<String> bean = new ArrayList<>();
		bean.add("a");
		bean.add("b");
		doReturn(bean).when(applicationContextMock).getBean(bean.getClass());

		SpringComponent subject = new SpringComponent(bean.getClass(), "clone()", "x") {};
		subject.setApplicationContext(applicationContextMock);
		subject.init(stormConf, topologyContextMock);

		assertArrayEquals("return", new Object[] {bean}, subject.invoke(new Object[0]));
		subject.setScatterOutput(true);
		assertArrayEquals("scattered return", bean.toArray(), subject.invoke(new Object[0]));
	}

	@Test
	public void filter() throws Exception {
		Iterator<?> beanMock = mock(Iterator.class);
		doReturn(beanMock).when(applicationContextMock).getBean(beanMock.getClass());

		SpringComponent subject = new SpringComponent(beanMock.getClass(), "next()", "x") {};
		subject.setApplicationContext(applicationContextMock);
		subject.setScatterOutput(true);
		subject.init(stormConf, topologyContextMock);

		assertEquals("return count", 0, subject.invoke(new Object[0]).length);

		doReturn(new Object()).when(beanMock).next();
		assertEquals("return count", 1, subject.invoke(new Object[0]).length);
	}

	@Test
	public void fieldDefaultAndCustomBinding() throws Exception {
		Values expected = new Values("Hello", "8");

		TestBean.Data data = new TestBean.Data();
		data.setId(8);
		data.setMessage("Hello");

		SpringComponent subject = new SpringComponent(TestBean.class, "greet(n)", "message", "number") {};
		subject.putOutputBinding("number", "id.toString()");

		assertEquals(expected, subject.getMapping(data, subject.getOutputFields()));
	}

	@Test
	public void fieldUnknownAndUnavailableAllowed() throws Exception {
		Values expected = new Values(null, 8, null);

		SpringComponent subject = new SpringComponent(TestBean.class, "greet(n)", "unknown", "pass", "setterOnly") {};
		subject.putOutputBinding("pass", "8");

		assertEquals(expected, subject.getMapping(new TestBean.Data(), subject.getOutputFields()));
	}

	@Test
	public void componentConfiguration() {
		SpringComponent subject = new SpringComponent(Collection.class, "clear()") {};
		assertNull(subject.getComponentConfiguration());
	}

}
