package eu.icolumbo.breeze.build;

import eu.icolumbo.breeze.SpringBolt;
import eu.icolumbo.breeze.SpringSpout;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Arrays.sort;
import static java.util.Collections.EMPTY_LIST;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;


/**
 * Tests {@link TopologyCompilation}.
 * @author Pascal S. de Kloe
 */
public class TopologyCompilationTest {

	TopologyCompilation subject = new TopologyCompilation();


	public static class Bean {
		public Map f() {return null;}
		public Map f(Object a) {return null;}
		public Map f(Object a, Object b) {return null;}
	}


	@Before
	public void init() {
		subject.clear();
	}

	@Test
	public void follow() {
		SpringSpout spout = new SpringSpout(Bean.class, "f()", "feed");
		SpringBolt bolt = new SpringBolt(Bean.class, "f(feed)", "ignored");
		test(spout, bolt);
	}

	@Test
	public void splitup() {
		SpringSpout spout = new SpringSpout(Bean.class, "f()", "feed");
		SpringBolt bolt1 = new SpringBolt(Bean.class, "f(feed)", "a");
		SpringBolt bolt2 = new SpringBolt(Bean.class, "f(feed)", "b");
		SpringBolt bolt3 = new SpringBolt(Bean.class, "f(b)", "y");
		SpringBolt bolt4 = new SpringBolt(Bean.class, "f(b)", "z");
		test(spout, bolt1, bolt2, bolt3, bolt4);
	}


	@Test
	public void join() {
		SpringSpout spout = new SpringSpout(Bean.class, "f()", "a", "b");
		SpringBolt bolt1 = new SpringBolt(Bean.class, "f(a)", "y");
		SpringBolt bolt2 = new SpringBolt(Bean.class, "f(b)", "z");
		SpringBolt bolt3 = new SpringBolt(Bean.class, "f(y, z)");
		test(spout, bolt1, bolt2, bolt3);
		assertPassThrough(subject.get(spout).get(0), "b");
		assertPassThrough(subject.get(spout).get(1), "y");
	}

	@Test
	public void collect() {
		SpringSpout spout = new SpringSpout(Bean.class, "f()", "feed");
		SpringBolt bolt1 = new SpringBolt(Bean.class, "f(feed)", "a");
		SpringBolt bolt2 = new SpringBolt(Bean.class, "f(a)", "b");
		SpringBolt bolt3 = new SpringBolt(Bean.class, "f(feed, a, b)");
		test(spout, bolt1, bolt2, bolt3);
		assertPassThrough(subject.get(spout).get(0), "feed");
		assertPassThrough(subject.get(spout).get(1), "feed", "a");
	}

	@Test
	public void reorder() {
		SpringSpout spout = new SpringSpout(Bean.class, "f()", "feed");
		SpringBolt bolt1 = new SpringBolt(Bean.class, "f(b)", "z");
		SpringBolt bolt2 = new SpringBolt(Bean.class, "f(feed)", "b");
		register(spout, bolt1, bolt2);
		subject.run();
		assertSequence(spout, bolt2, bolt1);
	}

	@Test
	public void incomplete() {
		SpringSpout spout = new SpringSpout(Bean.class, "f()", "feed");
		SpringBolt bolt = new SpringBolt(Bean.class, "f(b)", "z");
		register(spout, bolt);
		subject.run();
		assertSequence(spout);
	}

	@Test
	public void missingIds() {
		try {
			SpringSpout spout = mock(SpringSpout.class);
			subject.put(spout, EMPTY_LIST);
			fail("no exception");
		} catch (RuntimeException e) {
			assertEquals("Missing required component id", e.getMessage());
		}
	}

	private void test(SpringSpout spout, SpringBolt... boltSequence) {
		register(spout, boltSequence);
		subject.run();
		assertSequence(spout, boltSequence);
	}

	private void register(SpringSpout spout, SpringBolt... bolts) {
		spout.setId(Integer.toHexString(spout.hashCode()));
		for (SpringBolt b : bolts) b.setId(Integer.toHexString(b.hashCode()));

		subject.add(spout);
		subject.add(bolts);
	}

	private void assertSequence(SpringSpout spout, SpringBolt... bolts) {
		assertEquals("spout entry", true, subject.containsKey(spout));
		assertEquals("bolt sequence", asList(bolts), subject.get(spout));
	}

	private static void assertPassThrough(SpringBolt bolt, String... expected) {
		String[] actual = bolt.getPassThroughFields();
		sort(actual);
		sort(expected);
		assertArrayEquals(expected, actual);
	}

}
