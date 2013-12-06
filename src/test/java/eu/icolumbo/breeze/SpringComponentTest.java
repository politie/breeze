package eu.icolumbo.breeze;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


/**
 * Tests {@link SpringComponent}.
 * @author Pascal S. de Kloe
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
		assertArrayEquals(new String[]{"a", "b", "c"}, subject.outputFields);

		OutputFieldsDeclarer mock = mock(OutputFieldsDeclarer.class);
		subject.declareOutputFields(mock);

		ArgumentCaptor<Fields> captor = ArgumentCaptor.forClass(Fields.class);
		verify(mock).declare(captor.capture());
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

}
