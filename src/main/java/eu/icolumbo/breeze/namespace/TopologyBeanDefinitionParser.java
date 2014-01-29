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
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

import java.util.StringTokenizer;

import static org.springframework.util.StringUtils.hasText;
import static org.springframework.util.xml.DomUtils.getChildElementByTagName;
import static org.springframework.util.xml.DomUtils.getChildElementsByTagName;


/**
 * @author Jethro Bakker
 * @author Pascal S. de Kloe
 */
public class TopologyBeanDefinitionParser extends AbstractBeanDefinitionParser {

	@Override
	protected AbstractBeanDefinition parseInternal(Element element, ParserContext context) {
		BeanDefinitionRegistry registry = context.getRegistry();

		ManagedList<BeanDefinition> spoutDefinitions = new ManagedList<>();
		for (Element spoutElement : getChildElementsByTagName(element, "spout")) {
			BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(SpringSpout.class);
			spoutDefinitions.add(define(builder, spoutElement, registry));

			Element transElement = getChildElementByTagName(spoutElement, "transaction");
			if (transElement != null) {
				builder.addPropertyValue("ackSignature", transElement.getAttribute("ack"));
				builder.addPropertyValue("failSignature", transElement.getAttribute("fail"));
			}
		}

		ManagedList<BeanDefinition> boltDefinitions = new ManagedList<>();
		for (Element boltElement : getChildElementsByTagName(element, "bolt")) {
			BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(SpringBolt.class);
			builder.addPropertyValue("doAnchor", Boolean.valueOf(element.getAttribute("anchor")));
			boltDefinitions.add(define(builder, boltElement, registry));
		}

		for (Element rpcElement : getChildElementsByTagName(element, "rpc")) {
			BeanDefinitionBuilder spoutDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(SpringRPCRequest.class);
			spoutDefinitionBuilder.setScope("prototype");
			spoutDefinitionBuilder.addConstructorArgValue(rpcElement.getAttribute("signature"));
			spoutDefinitionBuilder.addPropertyValue("parallelism", Integer.valueOf(rpcElement.getAttribute("parallelism")));
			spoutDefinitions.add(spoutDefinitionBuilder.getBeanDefinition());

			BeanDefinitionBuilder boltDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(SpringRPCResponse.class);
			boltDefinitionBuilder.setScope("prototype");
			boltDefinitionBuilder.addConstructorArgValue(rpcElement.getAttribute("signature"));
			boltDefinitionBuilder.addConstructorArgValue(tokenize(rpcElement.getAttribute("outputFields")));
			boltDefinitionBuilder.addPropertyValue("parallelism", Integer.valueOf(rpcElement.getAttribute("parallelism")));
			boltDefinitions.add(boltDefinitionBuilder.getBeanDefinition());
		}

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(TopologyFactoryBean.class);
		builder.addPropertyValue("bolts", boltDefinitions);
		builder.addPropertyValue("spouts", spoutDefinitions);
		return builder.getBeanDefinition();
	}

	private static BeanDefinition define(BeanDefinitionBuilder builder, Element element, BeanDefinitionRegistry registry) {
		builder.setScope("prototype");

		builder.addConstructorArgValue(element.getAttribute("beanType"));
		builder.addConstructorArgValue(element.getAttribute("signature"));
		builder.addConstructorArgValue(tokenize(element.getAttribute("outputFields")));
		builder.addPropertyValue("parallelism", Integer.valueOf(element.getAttribute("parallelism")));
		builder.addPropertyValue("scatterOutput", Boolean.valueOf(element.getAttribute("scatterOutput")));

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
