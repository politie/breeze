[Spring](http://spring.io/) integration for [Storm](http://storm-project.net/)
------------------------------------------------------------------------------

Breeze binds Storm [topology components](http://github.com/nathanmarz/storm/wiki/Concepts) to [POJOs](http://en.wikipedia.org/wiki/Plain_Old_Java_Object). Write Spring beans and use them easily within Storm.

The `SpringSpout` and `SpringBolt` classes are configured with a Spring bean and a method signature. Each topology gets a dedicated application context.

While Storm has existing Spring integration support for launching topologies through [storm-spring](http://github.com/granthenke/storm-spring), this approach does not provide a Spring context to components such as Bolts and Spouts.

For each tuple request on `SpringSpout` and for each execute request on `SpringBolt` the bean's configured method is invoked. For bolts the function argument names are retrieved from the input tuple. The return value is emitted with the output field names.
* When no output fields are defined the return value is discarded.
* Single output field definitions mean that the return value is placed on the output tuple as is.
* In case of multiple output fields the result tuple mapping depends on its type. Maps are read by key and beans are read by property (getters).

Bolts and spouts may emit multiple tuples from a single call. When `#setScatterOutput(boolean)` has been set to `true` on either SpringSpout or SpringBolt then Breeze handles items from array and collection returns as separate emits. A `null` return means no emit in which case bolts act as a filter.

With `SpringBolt#setPassThroughFields(String...)` additional fields may be copied from the input tuple to the emit.

Storm's transaction architecture is honored with `#setAnchor(boolean)`.

Breeze currently only support shuffle stream grouping.


Get Started
===========

The [kickstarter project](https://github.com/internet-research-network/breeze-kickstarter) demonstrates how to define a topology with the [Breeze namespace](https://github.com/internet-research-network/breeze-kickstarter/blob/master/src/main/resources/applicationContext.xml) and [regular bean definitions](https://github.com/internet-research-network/breeze-kickstarter/blob/master/src/main/resources/demo-context.xml).

Maven
-----

```xml
<dependency>
	<groupId>eu.icolumbo.breeze</groupId>
	<artifactId>breeze</artifactId>
	<version>1.0.0</version>
</dependency>
```

Add the Clojars repository for Storm and the iRN repository for Breeze.

```xml
<repositories>
	<repository>
		<id>clojars</id>
		<url>http://clojars.org/repo</url>
	</repository>
	<repository>
		<id>irn</id>
		<url>https://raw.github.com/internet-research-network/repository/master</url>
	</repository>
</repositories>
```

The default topology starter can be used for local testing.
```xml
<plugin>
	<groupId>org.codehaus.mojo</groupId>
	<artifactId>exec-maven-plugin</artifactId>
	<version>1.1</version>
	<configuration>
		<mainClass>eu.icolumbo.breeze.namespace.TopologyStarter</mainClass>
		<arguments>
			<argument>demo</argument>
		</arguments>
		<systemProperties>
			<property>
				<key>localRun</key>
			</property>
		</systemProperties>
	</configuration>
</plugin>
```


Contributors
============

* Pascal de Kloe [@GitHub](http://github.com/pascaldekloe)
* Jethro Bakker [@GitHub](http://github.com/jethrobakker)
* Jasper van Veghel [@GitHub](http://github.com/jaspervanveghel)
