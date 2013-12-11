package eu.icolumbo.breeze.namespace;

import eu.icolumbo.breeze.SpringBolt;
import eu.icolumbo.breeze.SpringSpout;
import eu.icolumbo.breeze.build.TopologyFactoryBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

import java.util.StringTokenizer;
import java.util.UUID;

import static org.springframework.util.StringUtils.hasText;
import static org.springframework.util.xml.DomUtils.getChildElementsByTagName;


/**
 * @author Jethro Bakker
 * @author Pascal S. de Kloe
 */
public class TopologyBeanDefinitionParser extends AbstractBeanDefinitionParser {

	private static final Logger logger = LoggerFactory.getLogger(TopologyBeanDefinitionParser.class);


	@Override
	protected AbstractBeanDefinition parseInternal(Element element, ParserContext context) {
		BeanDefinitionRegistry registry = context.getRegistry();

		ManagedList <BeanDefinition> spoutDefinitions = new ManagedList<>();
		for (Element spoutElement : getChildElementsByTagName(element, "spout")) {
			BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(SpringSpout.class);
			spoutDefinitions.add(define(builder, spoutElement, registry));
		}

		ManagedList <BeanDefinition> boltDefinitions = new ManagedList<>();
		for (Element boltElement : getChildElementsByTagName(element, "bolt")) {
			BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(SpringBolt.class);
			builder.addPropertyValue("doAnchor", Boolean.valueOf(element.getAttribute("anchor")));
			boltDefinitions.add(define(builder, boltElement, registry));
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

		String id = element.getAttribute(ID_ATTRIBUTE);
		if (! hasText(id)) {
			id = UUID.randomUUID().toString();
			logger.warn("Generated id '{}' for {}", id, element.getTagName());
		}
		builder.addPropertyValue("id", id);

		AbstractBeanDefinition definition = builder.getBeanDefinition();
		registry.registerBeanDefinition(id, definition);

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
