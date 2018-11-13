package eu.icolumbo.breeze.build;

import eu.icolumbo.breeze.ConfiguredBolt;
import eu.icolumbo.breeze.ConfiguredSpout;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.springframework.beans.factory.FactoryBean;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author Pascal S. de Kloe
 */
public class TopologyFactoryBean extends TopologyCompilation implements FactoryBean<StormTopology> {

	private StormTopology singleton;


	public void setSpouts(List<ConfiguredSpout> value) {
		for (ConfiguredSpout spout : value)
			add(spout);
	}

	public void setBolts(List<ConfiguredBolt> value) {
		for (ConfiguredBolt bolt : value)
			add(bolt);
	}

	@Override
	public Class<StormTopology> getObjectType() {
		return StormTopology.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public StormTopology getObject() throws Exception {
		if (singleton == null)
			singleton = build();
		return singleton;
	}

	private StormTopology build() {
		run();
		verify();

		Map<String,BoltDeclarer> declaredBolts = new HashMap<>();

		TopologyBuilder builder = new TopologyBuilder();
		for (Map.Entry<ConfiguredSpout,List<ConfiguredBolt>> line : entrySet()) {
			ConfiguredSpout spout = line.getKey();
			String lastId = spout.getId();
			String streamId = spout.getOutputStreamId();
			builder.setSpout(lastId, spout, spout.getParallelism());
			for (ConfiguredBolt bolt : line.getValue()) {
				String id = bolt.getId();
				BoltDeclarer declarer = declaredBolts.get(id);
				if (declarer == null)
					declarer = builder.setBolt(id, bolt, bolt.getParallelism());
				declarer.noneGrouping(lastId, streamId);
				if (declaredBolts.put(id, declarer) != null) break;
				lastId = id;
				streamId = bolt.getOutputStreamId();
			}
		}

		return builder.createTopology();
	}

}
