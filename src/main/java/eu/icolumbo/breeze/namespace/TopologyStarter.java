package eu.icolumbo.breeze.namespace;

import eu.icolumbo.breeze.SingletonApplicationContext;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.validation.ConfigValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.lang.System.err;
import static java.lang.System.exit;
import static java.util.Arrays.asList;
import static org.springframework.util.StringUtils.hasText;


/**
 * Storm bootstrap.
 * @author Pascal S. de Kloe
 */
public class TopologyStarter extends Thread {

	public static final String MAIN_CONTEXT = "classpath:/applicationContext.xml";
	public static final String LOCAL_RUN_PARAM = "localRun";
	public static final String LOCAL_RUN_DEFAULT_TIMEOUT = "10";

	private static final Logger logger = LoggerFactory.getLogger(TopologyStarter.class);
	private static final String CONFIGURATION_TYPE_FIELD_SUFFIX = "_SCHEMA";
	private static final String LIST_CONTINUATION_PATTERN = ",\\s*";

	private final String ID;
	private final Properties properties;


	public TopologyStarter(String topologyId, Properties setup) {
		super("starter-" + topologyId);
		ID = topologyId;
		properties = setup;
	}

	public static void main(String[] args) throws IOException {
		logger.info("Breeze bootstrap called with {}", args);
		if (args.length == 0) {
			printHelp();
			exit(1);
		}

		Properties setup = new Properties();
		try {
			for (int i = 1; i < args.length; ++i) {
				String filePath = args[i];
				logger.debug("Configuration file {} at {}", i, filePath);

				InputStream stream = new FileInputStream(filePath);
				setup.load(stream);
				stream.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
			exit(2);
		}

		new TopologyStarter(args[0], setup).start();
	}

	private static void printHelp() {
		err.println("BREEZE TOPOLOGY STARTER");
		err.println("Expected arguments: topology-id [FILE]... [OPTION]...");
		err.println();
		err.println("The file arguments should point to Java property files.");
		err.println();
		err.println("OPTIONS");
		err.println();
		err.println("\t-D" + LOCAL_RUN_PARAM + "[=timeout]");
		err.println("\t\tTests the topology locally for a number of seconds." +
				" The default is " + LOCAL_RUN_DEFAULT_TIMEOUT);
	}

	@Override
	public void run() {
		properties.put(Config.TOPOLOGY_NAME, ID);
		Config config = stormConfig(properties);
		ApplicationContext spring = SingletonApplicationContext.loadXml(config, MAIN_CONTEXT);
		try {
			StormTopology topology = spring.getBean(ID, StormTopology.class);

			Properties systemProperties = System.getProperties();
			if (systemProperties.containsKey(LOCAL_RUN_PARAM)) {
				String timeout = systemProperties.getProperty(LOCAL_RUN_PARAM);
				if (! hasText(timeout))
					timeout = LOCAL_RUN_DEFAULT_TIMEOUT;
				long ms = 1000L * Integer.parseInt(timeout);

				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(ID, config, topology);
				sleep(ms);
				cluster.shutdown();
			} else
				StormSubmitter.submitTopology(ID, config, topology);
		} catch (Exception e) {
			e.printStackTrace();
			exit(255);
		}
	}

	/**
	 * Applies type conversion where needed.
	 * @return a copy of source ready for Storm.
	 * @see <a href="https://issues.apache.org/jira/browse/STORM-173">Strom issue 173</a>
	 */
	public static Config stormConfig(Properties source) {
		Config result = new Config();

		logger.debug("Mapping declared types for Storm properties...");
		for (Field field : result.getClass().getDeclaredFields()) {
			if (field.getType() != String.class) continue;
			if (field.getName().endsWith(CONFIGURATION_TYPE_FIELD_SUFFIX)) continue;

			try {
				String key = field.get(result).toString();
				String entry = source.getProperty(key);
				if (entry == null) continue;

				String typeFieldName = field.getName() + CONFIGURATION_TYPE_FIELD_SUFFIX;
				Field typeField = result.getClass().getDeclaredField(typeFieldName);
				Object type = typeField.get(result);

				logger.trace("Detected key '{}' as: {}", key, field);
				Object value = null;
				if (type == String.class)
					value = entry;
				if (type == ConfigValidation.IntegerValidator.class || type == ConfigValidation.PowerOf2Validator.class)
					value = Integer.valueOf(entry);
				if (type == Boolean.class)
					value = Boolean.valueOf(entry);
				if (type == ConfigValidation.StringOrStringListValidator.class)
					value = asList(entry.split(LIST_CONTINUATION_PATTERN));

				if (value == null) {
					logger.warn("No parser for key '{}' type: {}", key, typeField);
					value = entry;
				}
				result.put(key, value);
			} catch (ReflectiveOperationException e) {
				logger.debug("Interpretation failure on {}: {}", field, e);
			}
		}

		// Copy remaining
		for (Map.Entry<Object,Object> e : source.entrySet()) {
			String key = e.getKey().toString();
			if (result.containsKey(key)) continue;
			result.put(key, e.getValue());
		}

		return result;
	}

}
