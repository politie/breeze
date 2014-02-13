package eu.icolumbo.breeze;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Collections.checkedMap;
import static org.springframework.util.StringUtils.hasText;


/**
 * Spring registry.
 * @author Pascal S. de Kloe
 * @author Jethro Bakker
 */
public enum SingletonApplicationContext {

	INSTANCE;

	private static final Logger logger = LoggerFactory.getLogger(SingletonApplicationContext.class);

	private final Map<String,ApplicationContext> registry = new HashMap<>();


	/**
	 * Gets the Spring setup for the respective Storm topology.
	 */
	public static synchronized ApplicationContext get(Map stormConf, TopologyContext topologyContext) {
		String topologyName = (String) stormConf.get(Config.TOPOLOGY_NAME);
		if (! hasText(topologyName)) {
			String msg = "Missing required '" + Config.TOPOLOGY_NAME + "' in Storm configuration";
			throw new IllegalStateException(msg);
		}

		logger.debug("Application context lookup for topology '{}'", topologyName);

		ApplicationContext entry = INSTANCE.registry.get(topologyName);
		if (entry == null) {
			logger.debug("Need new application context");
			entry = loadXml(stormConf, format("classpath:/%s-context.xml", topologyName));
			logger.info("Application context instantiated for topology '{}'", topologyName);
			INSTANCE.registry.put(topologyName, entry);
		}

		return entry;
	}

	/**
	 * Instantiates a new Application Context.
	 * @param stormConf the Storm properties to expose to Spring.
	 * @param location the classpath URI.
	 */
	public static ApplicationContext loadXml(Map stormConf, String location) {
		String[] locations = {location};
		AbstractApplicationContext result = new ClassPathXmlApplicationContext(locations, false);
		result.setId(location);

		if (! stormConf.isEmpty()) {
			logger.debug("Providing Storm properties to '{}': {}", location, stormConf);
			Map<String,Object> checked = checkedMap(stormConf, String.class, Object.class);
			PropertySource source = new MapPropertySource("storm-configuration", checked);
			result.getEnvironment().getPropertySources().addLast(source);
		}

		result.refresh();
		return result;
	}

}
