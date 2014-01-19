package eu.icolumbo.breeze.build;

import eu.icolumbo.breeze.SpringBolt;
import eu.icolumbo.breeze.SpringComponent;
import eu.icolumbo.breeze.SpringSpout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static java.util.Arrays.asList;
import static java.util.Collections.addAll;


/**
 * Performs dependency calculation.
 * @author Pascal S. de Kloe
 */
public class TopologyCompilation extends TreeMap<SpringSpout,List<SpringBolt>> implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(TopologyCompilation.class);

	private final List<SpringBolt> unbound = new ArrayList<>();


	/**
	 * Default constructor.
	 */
	public TopologyCompilation() {
		super(new Comparator<SpringComponent>() {

			@Override
			public int compare(SpringComponent o1, SpringComponent o2) {
				String id1 = o1.getId();
				String id2 = o2.getId();
				return id1.compareTo(id2);
			}

		});
	}

	public void add(SpringSpout... values) {
		for (SpringSpout spout : values)
			put(spout, new ArrayList<SpringBolt>());
	}

	public void add(SpringBolt... values) {
		addAll(unbound, values);
	}

	@Override
	public void clear() {
		super.clear();
		unbound.clear();
	}

	/**
	 * Checks whether {@link #run() the complitation} succeeded.
	 */
	public void verify() throws IllegalStateException {
		if (unbound.isEmpty()) return;
		String msg = "Can't resolve all input fields for: " + unbound;
		throw new IllegalStateException(msg);
	}

	@Override
	public void run() {
		logger.debug("Matching {} spouts with {} bolts", size(), unbound.size());
		for (Map.Entry<SpringSpout,List<SpringBolt>> line : entrySet()) {
			Set<String> availableFields = new HashSet<>();
			addAll(availableFields, line.getKey().getOutputFields());

			List<SpringBolt> options = new ArrayList<>(unbound);
			for (boolean collected = true; collected; ) {
				collected = false;
				Iterator<SpringBolt> todo = options.iterator();
				while (todo.hasNext()) {
					SpringBolt option = todo.next();
					logger.trace("Trying {} for {}", option, line.getKey());
					if (availableFields.containsAll(asList(option.getInputFields()))) {
						line.getValue().add(option);
						addAll(availableFields, option.getOutputFields());
						todo.remove();
						collected = true;
					}
				}
			}

			logger.debug("Found {} bolts for {}", line.getValue().size(), line.getKey());
		}

		for (List<SpringBolt> processed: values()) {
			unbound.removeAll(processed);

			Set<String> requiredFields = new HashSet<>();
			for (int i = processed.size(); --i >= 0; ) {
				SpringBolt bolt = processed.get(i);
				requiredFields.removeAll(asList(bolt.getOutputFields()));
				bolt.setPassThroughFields(requiredFields.toArray(new String[requiredFields.size()]));
				addAll(requiredFields, bolt.getInputFields());
			}
		}

		logger.info("Compiled as: {}", this);
	}

}
