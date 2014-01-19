package eu.icolumbo.breeze.build;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import eu.icolumbo.breeze.SpringBolt;
import eu.icolumbo.breeze.SpringSpout;
import org.springframework.beans.factory.FactoryBean;

import java.util.List;
import java.util.Map;


/**
 * @author Pascal S. de Kloe
 */
public class TopologyFactoryBean extends TopologyCompilation implements FactoryBean<StormTopology> {

	private StormTopology singleton;


	public void setSpouts(List<SpringSpout> value) {
		for (SpringSpout spout : value)
			add(spout);
	}

	public void setBolts(List<SpringBolt> value) {
		for (SpringBolt bolt : value)
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

		TopologyBuilder builder = new TopologyBuilder();
		for (Map.Entry<SpringSpout,List<SpringBolt>> line : entrySet()) {
			SpringSpout spout = line.getKey();
			String lastId = spout.getId();
			String streamId = spout.getOutputStreamId();
			builder.setSpout(lastId, spout, spout.getParallelism());
			for (SpringBolt bolt : line.getValue()) {
				String id = bolt.getId();
				BoltDeclarer declarer = builder.setBolt(id, bolt, bolt.getParallelism());
				declarer.noneGrouping(lastId, streamId);
				lastId = id;
				streamId = bolt.getOutputStreamId();
			}
		}

		return builder.createTopology();
	}

}
