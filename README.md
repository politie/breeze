[Spring](http://spring.io/) integration for [Storm](http://storm-project.net/)
------------------------------------------------------------------------------

Breeze binds Storm [topology components](http://github.com/nathanmarz/storm/wiki/Concepts) to [POJOs](http://en.wikipedia.org/wiki/Plain_Old_Java_Object).
The `SpringSpout` and `SpringBolt` classes are configured with a Spring bean and a method signature. Each topology gets a dedicated application context.

While Storm has existing Spring integration support for launching topologies through [storm-spring](http://github.com/granthenke/storm-spring), this approach does not provide a Spring context to components such as Bolts and Spouts.

For each tuple request on `SpringSpout` and for each execute request on `SpringBolt` the bean's configured method is invoked. For bolts the function argument names are retrieved from the input tuple. The return value is emitted with the output field names.
When no output fields are defined the return value is discarded. Single output field definitions mean that the return value is placed on the output tuple as is. In case of multiple output fields the result tuple mapping depends on its type. For now only maps are supported.

Bolts and spouts may emit multiple tuples from a single call. When `#setScatterOutput(boolean)` has been set to `true` on either SpringSpout or SpringBolt then Breeze handles items from array and collection returns as separate emits. A `null` return means no emit in which case bolts act as a filter.

With `SpringBolt#setPassThroughFields(String...)` additional fields may be copied from the input tuple to the emit.

Storm's transaction architecture is honored with `#setAnchor(boolean)`.


Get Started
===========

Storm topologies can be defined with Java.

```java
package com.example;

import eu.icolumbo.analytics.spring.SpringBolt;
import eu.icolumbo.analytics.spring.SpringSpout;

import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

import java.util.HashMap;
import java.util.Map;


public class TopologyStarter {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		SpringSpout spout = new SpringSpout(ChunkReader.class, "next()", "document");
		spout.setScatterOutput(true);
		builder.setSpout("chunk-reader", spout);

		SpringBolt bolt1 = new SpringBolt(Analyser.class, "analyze(document)", "analysis");
		bolt1.setPassThroughFields("document");
		builder.setBolt("analyser", bolt1).noneGrouping("chunk-reader");

		SpringBolt bolt2 = new SpringBolt(AnalyticsRepo.class, "register(document, analysis)");
		builder.setBolt("analytics-repo", bolt2).noneGrouping("analyser");

		StormTopology topology = builder.createTopology();

		Map<String, Object> config = new HashMap<>();
		config.put("batch-file", args[0]);

		StormSubmitter.submitTopology("example", config, topology);
	}

}
```

Breeze will look for a matching beans definition based on the topology ID at `classpath:/demo-context.xml`. The Storm configuration map is available as a property source to the Spring enviroment.

```xml
<?xml version="1.0" encoding="US-ASCII"?>
<beans xmlns="http://www.springframework.org/schema/beans"
		xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
		xmlns:context="http://www.springframework.org/schema/context"
		xsi:schemaLocation="
			http://www.springframework.org/schema/beans
			http://www.springframework.org/schema/beans/spring-beans.xsd
			http://www.springframework.org/schema/context
			http://www.springframework.org/schema/context/spring-context.xsd
		">

	<context:property-placeholder/>

	<bean class="com.example.ChunkReader">
		<property name="feed" value="${batch-file}"/>
	</bean>

	<bean class="com.example.Analyser" scope="prototype"/>

	<bean class="com.example.AnalyticsRepo"/>

</beans>
```

Note that the Analyser bean will be instantiated for each call/tuple due to the prototype scope.

```shell
$ /opt/storm/bin/storm jar ./demo-dist.jar com.example.TopologyStarter /var/spool/feed/batch1
```


Contributors
============

* Pascal de Kloe [@GitHub](http://github.com/pascaldekloe)
* Jethro Bakker [@GitHub](http://github.com/jethrobakker)
* Jasper van Veghel [@GitHub](http://github.com/jaspervanveghel)
