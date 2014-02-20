[Spring](http://spring.io/) integration for [Storm](http://storm-project.net/)
------------------------------------------------------------------------------

Breeze binds Storm [topology components](http://github.com/nathanmarz/storm/wiki/Concepts) to [POJOs](http://en.wikipedia.org/wiki/Plain_Old_Java_Object). Write Spring beans and use them easily within a cluster.

The `SpringSpout` and `SpringBolt` classes are configured with a Spring bean and a method signature. The compiler automagically orders the processing steps based on the field names.
Each topology gets a dedicated application context.

Breeze currently supports ["none" grouping](http://github.com/nathanmarz/storm/wiki/Concepts#stream-groupings) only.


Get Started
===========

The [kickstarter project](https://github.com/internet-research-network/breeze-kickstarter) demonstrates how to define a topology with the [Breeze namespace](https://github.com/internet-research-network/breeze-kickstarter/blob/master/src/main/resources/applicationContext.xml) and [regular bean definitions](https://github.com/internet-research-network/breeze-kickstarter/blob/master/src/main/resources/demo-context.xml).

Maven
-----

```xml
<dependency>
	<groupId>eu.icolumbo.breeze</groupId>
	<artifactId>breeze</artifactId>
	<version>1.2.0</version>
</dependency>
```

Add the Clojars repository for Storm.

```xml
<repositories>
	<repository>
		<id>clojars</id>
		<url>http://clojars.org/repo</url>
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


Output Binding
==============

For each read request on `SpringSpout` and for each execute request on `SpringBolt` the bean's configured method is invoked.

The scatter feature can split returned arrays and collections into multiple emissions. With scatter enabled a `null` return means no emit in which case bolts can act as a filter.

When no output fields are defined the return value is discarded. By default a single output field gives the return value as is. In case of multiple output fields the return value is read by property (getter).
More complicated bindings may be defined with [SpEL](http://docs.spring.io/spring/docs/current/spring-framework-reference/html/expressions.html) as shown below.

```xml
<storm:bolt beanType="com.example.EntityExtractor" signature="read(doc)"
		outputFields="nameCount names source">
	<breeze:field name="nameCount" expression="Names.Size()"/>
	<breeze:field name="source" expression="'x-0.9'"/>
</breeze:bolt>
```

Exceptions can be configured to cause a read delay.

```xml
<breeze:spout id="dumpFeed" beanType="com.example.DumpReader" signature="read()" outputFields="record">
	<storm:exception type="java.nio.BufferUnderflowException" delay="500"/>
</breeze:spout>
```


Transactions
============

Storm supports [guaranteed message processing](https://github.com/nathanmarz/storm/wiki/Guaranteeing-message-processing). Breeze provides this functionality with an ack and/or fail method signature.
With the following example configuration `DumpReader#ok` is called with the hash code on succes and errors are reported with the value of `getSerial()` on record at `DumpReader#bad`.

```xml
<breeze:spout id="dumpFeed" beanType="com.example.DumpReader" signature="read()" outputFields="record">
	<breeze:field name="hash" expression="hashCode()"/>
	<breeze:transaction ack="ok(hash)" fail="bad(serial)"/>
</breeze:spout>
```


RPC
===

Storm's [Distributed RPC or DRPC](https://github.com/nathanmarz/storm/wiki/Distributed-RPC) can also be configured with the beans XML extension.

```xml
<breeze:topology id="demo">
	<breeze:rpc signature="dgreet(s)" outputFields="greeting"/>
	<breeze:bolt beanType="com.example.Greeter" signature="greet(s)" outputFields="greeting"/>
</breeze:topology>
```

```java
DRPCClient client = new DRPCClient("storm1.example.com", 3772);
String result = client.execute("dgreet", "World");
System.err.println(result);
```


Contributors
============

* Pascal de Kloe [@GitHub](http://github.com/pascaldekloe)
* Jethro Bakker [@GitHub](http://github.com/jethrobakker)
* Jasper van Veghel [@GitHub](http://github.com/jaspervanveghel)
* Sonatype [central repository support](https://issues.sonatype.org/browse/OSSRH-8126)
