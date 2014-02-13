package eu.icolumbo.breeze.namespace;

import eu.icolumbo.breeze.SpringBolt;
import eu.icolumbo.breeze.connect.SpringRPCRequest;
import eu.icolumbo.breeze.connect.SpringRPCResponse;
import eu.icolumbo.breeze.SpringSpout;
import eu.icolumbo.breeze.build.TopologyFactoryBean;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionValidationException;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import static org.springframework.beans.factory.support.BeanDefinitionBuilder.rootBeanDefinition;
import static org.springframework.util.StringUtils.hasText;
import static org.springframework.util.xml.DomUtils.getChildElementByTagName;
import static org.springframework.util.xml.DomUtils.getChildElementsByTagName;


/**
 * @author Jethro Bakker
 * @author Pascal S. de Kloe
 */
public class TopologyBeanDefinitionParser extends AbstractBeanDefinitionParser {

	@Override
	protected AbstractBeanDefinition parseInternal(Element root, ParserContext context) {
		BeanDefinitionRegistry registry = context.getRegistry();

		ManagedList<BeanDefinition> spoutDefinitions = new ManagedList<>();
		for (Element spout : getChildElementsByTagName(root, "spout")) {
			BeanDefinitionBuilder builder = rootBeanDefinition(SpringSpout.class);

			Map<Class<? extends Exception>,Long> delayExceptions = new HashMap<>();
			for (Element exception : getChildElementsByTagName(spout, "exception")) {
				String className = exception.getAttribute("type");
				Class<? extends Exception> type = null;
				try {
					type = (Class<? extends Exception>) Class.forName(className);
				} catch (ClassNotFoundException e) {
					String msg = "No such class: " + className;
					throw new IllegalStateException(msg, e);
				}
				delayExceptions.put(type, Long.valueOf(exception.getAttribute("delay")));
			}
			builder.addPropertyValue("delayExceptions", delayExceptions);

			Element transaction = getChildElementByTagName(spout, "transaction");
			if (transaction != null) {
				builder.addPropertyValue("ackSignature", transaction.getAttribute("ack"));
				builder.addPropertyValue("failSignature", transaction.getAttribute("fail"));
			}

			spoutDefinitions.add(define(builder, spout, registry));
		}

		ManagedList<BeanDefinition> boltDefinitions = new ManagedList<>();
		for (Element bolt : getChildElementsByTagName(root, "bolt")) {
			BeanDefinitionBuilder builder = rootBeanDefinition(SpringBolt.class);
			builder.addPropertyValue("doAnchor", Boolean.valueOf(root.getAttribute("anchor")));
			boltDefinitions.add(define(builder, bolt, registry));
		}

		for (Element rpc : getChildElementsByTagName(root, "rpc")) {
			BeanDefinitionBuilder spoutDef = rootBeanDefinition(SpringRPCRequest.class);
			spoutDef.setScope("prototype");
			spoutDef.addConstructorArgValue(rpc.getAttribute("signature"));
			spoutDef.addPropertyValue("parallelism", Integer.valueOf(rpc.getAttribute("parallelism")));
			spoutDefinitions.add(spoutDef.getBeanDefinition());

			BeanDefinitionBuilder boltDef = rootBeanDefinition(SpringRPCResponse.class);
			boltDef.setScope("prototype");
			boltDef.addConstructorArgValue(rpc.getAttribute("signature"));
			boltDef.addConstructorArgValue(tokenize(rpc.getAttribute("outputFields")));
			boltDef.addPropertyValue("parallelism", Integer.valueOf(rpc.getAttribute("parallelism")));
			boltDefinitions.add(boltDef.getBeanDefinition());
		}

		BeanDefinitionBuilder builder = rootBeanDefinition(TopologyFactoryBean.class);
		builder.addPropertyValue("bolts", boltDefinitions);
		builder.addPropertyValue("spouts", spoutDefinitions);
		return builder.getBeanDefinition();
	}

	private static BeanDefinition
	define(BeanDefinitionBuilder builder, Element element, BeanDefinitionRegistry registry) {
		builder.setScope("prototype");

		builder.addConstructorArgValue(element.getAttribute("beanType"));
		builder.addConstructorArgValue(element.getAttribute("signature"));
		builder.addConstructorArgValue(tokenize(element.getAttribute("outputFields")));
		builder.addPropertyValue("parallelism", Integer.valueOf(element.getAttribute("parallelism")));
		builder.addPropertyValue("scatterOutput", Boolean.valueOf(element.getAttribute("scatterOutput")));

		Map<String,String> outputBinding = new HashMap<>();
		for (Element field : getChildElementsByTagName(element, "field"))
			outputBinding.put(field.getAttribute("name"), field.getAttribute("expression"));
		builder.addPropertyValue("outputBinding", outputBinding);

		AbstractBeanDefinition definition = builder.getBeanDefinition();

		String id = element.getAttribute(ID_ATTRIBUTE);
		if (hasText(id)) {
			builder.addPropertyValue("id", id);
			registry.registerBeanDefinition(id, definition);
		}

		return definition;
	}

	private static String[] tokenize(String tokens) {
		StringTokenizer parser = new StringTokenizer(tokens);
		String[] array = new String[parser.countTokens()];
		for (int i = 0; parser.hasMoreTokens();
				array[i++] = parser.nextToken());
		return array;
	}

}
