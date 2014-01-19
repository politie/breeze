package eu.icolumbo.breeze.build;

import eu.icolumbo.breeze.SpringBolt;
import eu.icolumbo.breeze.SpringSpout;

import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.sort;
import static java.util.Collections.EMPTY_LIST;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
		public void g(Object a) {}
	}


	@Before
	public void init() {
		subject.clear();
	}

	@Test
	public void follow() {
		subject.add(spout("s1", "f()", "feed"));
		subject.add(bolt("b1", "f(feed)", "ignored"));
		subject.run();
		subject.verify();

		assertPipeline("s1", "b1");
		assertPassThrough("b1");
	}

	@Test
	public void splitUp() {
		subject.add(spout("s1", "f()", "feed"));
		subject.add(bolt("b1", "f(feed)", "a"));
		subject.add(bolt("b2", "f(feed)", "b"));
		subject.add(bolt("b3", "f(b)", "y"));
		subject.add(bolt("b4", "f(b)", "z"));

		assertPipeline("s1", "b1", "b2", "b3", "b4");
		assertPassThrough("b1", "feed");
		assertPassThrough("b2");
		assertPassThrough("b3", "b");
		assertPassThrough("b4");
	}


	@Test
	public void join() {
		subject.add(spout("s1", "f()", "a", "b"));
		subject.add(bolt("b1", "f(a)", "y"));
		subject.add(bolt("b2", "f(b)", "z"));
		subject.add(bolt("b3", "f(y, z)"));

		assertPipeline("s1", "b1", "b2", "b3");
		assertPassThrough("b1", "b");
		assertPassThrough("b2", "y");
		assertPassThrough("b3");
	}

	@Test
	public void order() {
		subject.add(spout("s1", "f()", "feed"));
		subject.add(bolt("b1", "f(feed, a, b)"));
		subject.add(bolt("b2", "f(a)", "b"));
		subject.add(bolt("b3", "f(feed)", "a"));

		assertPipeline("s1", "b3", "b2", "b1");
		assertPassThrough("b3", "feed");
		assertPassThrough("b2", "feed", "a");
		assertPassThrough("b1");
	}

	@Test
	public void voidPassThrough() {
		subject.add(spout("s1", "f()", "y"));
		subject.add(bolt("b1", "g(y)"));
		subject.add(bolt("b2", "g(y)"));

		assertPipeline("s1", "b1", "b2");
		assertPassThrough("b1", "y");
		assertPassThrough("b2");
	}

	@Test
	public void incomplete() {
		subject.add(spout("s1", "f()", "feed"));
		subject.add(bolt("b1", "f(b)", "z"));
		subject.run();

		try {
			subject.verify();
			fail("no verify exception");
		} catch (IllegalStateException e) {
			String expected = "Can't resolve all input fields for: [SpringBolt 'b1']";
			assertEquals(expected, e.getMessage());
		}
	}

	private void assertPipeline(String... expectedIdSequence) {
		subject.run();
		subject.verify();

		String spoutId = expectedIdSequence[0];
		SpringSpout spout = spoutById(spoutId);

		List<SpringBolt> bolts = subject.get(spout);
		int boltCount = bolts.size();

		String[] actualIdSequence = new String[boltCount + 1];
		actualIdSequence[0] = spoutId;
		for (int i = 0; i < boltCount; ) {
			String id = bolts.get(i).getId();
			actualIdSequence[++i] = id;
		}

		assertArrayEquals("component ID", expectedIdSequence, actualIdSequence);
	}

	private void assertPassThrough(String boltId, String... fieldNames) {
		SpringBolt bolt = boltById(boltId);

		Set<String> expected = new HashSet<>();
		Set<String> actual = new HashSet<>();
		Collections.addAll(expected, fieldNames);
		Collections.addAll(actual, bolt.getPassThroughFields());
		assertEquals("field names", expected, actual);
	}

	private SpringSpout spoutById(String id) {
		Set<SpringSpout> availableSpouts = subject.keySet();
		for (SpringSpout key : availableSpouts)
			if (id.equals(key.getId()))
				return key;

		fail("missing spout " + id + " in " + availableSpouts);
		return null;
	}

	private SpringBolt boltById(String id) {
		Collection<List<SpringBolt>> availableBolts = subject.values();
		for (List<SpringBolt> boltSequence : availableBolts)
			for (SpringBolt entry : boltSequence)
				if (id.equals(entry.getId()))
					return entry;

		fail("missing bolt " + id + " in " + availableBolts);
		return null;
	}

	private static SpringSpout spout(String id, String signature, String... outputFields) {
		SpringSpout spout = new SpringSpout(Bean.class, signature, outputFields);
		spout.setId(id);
		return spout;
	}

	private static SpringBolt bolt(String id, String signature, String... outputFields) {
		SpringBolt bolt = new SpringBolt(Bean.class, signature, outputFields);
		bolt.setId(id);
		return bolt;
	}

}
