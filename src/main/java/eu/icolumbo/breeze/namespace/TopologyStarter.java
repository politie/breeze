package eu.icolumbo.breeze.namespace;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static java.lang.System.err;
import static java.lang.System.exit;
import static org.springframework.util.StringUtils.hasText;


/**
 * @author Pascal S. de Kloe
 */
public class TopologyStarter extends Thread {

	public static final String MAIN_CONTEXT = "classpath:/applicationContext.xml";
	public static final String LOCAL_RUN_PARAM = "localRun";
	public static final String LOCAL_RUN_DEFAULT_TIMEOUT = "10";

	private final ApplicationContext spring = new ClassPathXmlApplicationContext(MAIN_CONTEXT);
	private final String ID;
	private final Properties config = new Properties();


	public TopologyStarter(String topologyId) {
		super("starter-" + topologyId);
		ID = topologyId;
	}

	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			printHelp();
			exit(1);
		}

		TopologyStarter starter = new TopologyStarter(args[0]);

		try {
			for (int i = 1; i < args.length; ++i) {
				InputStream stream = new FileInputStream(args[i]);
				starter.config.load(stream);
				stream.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
			exit(2);
		}

		starter.start();
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
		err.println("\t\tTests the topology for a number of seconds. The default is " + LOCAL_RUN_DEFAULT_TIMEOUT);
	}

	@Override
	public void run() {
		config.put(Config.TOPOLOGY_NAME, ID);
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

}
