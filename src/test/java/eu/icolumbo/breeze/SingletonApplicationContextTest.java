package eu.icolumbo.breeze;

import org.apache.storm.task.TopologyContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.ApplicationContext;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


/**
 * Tests {@link SingletonApplicationContext}.
 * @author Pascal S. de Kloe
 * @author Jethro Bakker
 */
@RunWith(MockitoJUnitRunner.class)
public class SingletonApplicationContextTest {

	@Mock
	TopologyContext topologyContextMock;

	Map<String,Object> stormConf = new HashMap<>();

	@Before
	public void init() {
		stormConf.clear();
	}

	@Test
	public void xmlContext() {
		stormConf.put("topology.name", "simple");

		ApplicationContext result = SingletonApplicationContext.get(stormConf, topologyContextMock);
		assertEquals("classpath:/simple-context.xml", result.getId());

		TestBean bean = result.getBean(TestBean.class);
		assertEquals("Hello", bean.getGreeting());
	}

	@Test(expected=BeanDefinitionStoreException.class)
	public void noContext() {
		stormConf.put("topology.name", "unkown");
		SingletonApplicationContext.get(stormConf, topologyContextMock);
	}

	@Test(expected=IllegalStateException.class)
	public void noConfig() {
		SingletonApplicationContext.get(stormConf, topologyContextMock);
	}

	@Test
	public void stormProperties() {
		Object expected = "Hello";
		stormConf.put("greeting", expected);
		stormConf.put("topology.name", "properties");

		ApplicationContext result = SingletonApplicationContext.get(stormConf, topologyContextMock);
		assertEquals("classpath:/properties-context.xml", result.getId());

		TestBean bean = result.getBean(TestBean.class);
		assertEquals(expected, bean.getGreeting());
	}

	@Test
	public void single() {
		assertEquals(SingletonApplicationContext.INSTANCE, SingletonApplicationContext.valueOf("INSTANCE"));
		assertEquals(1, SingletonApplicationContext.values().length);
	}

}
