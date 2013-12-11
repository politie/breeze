package eu.icolumbo.breeze.namespace;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;


/**
 * @author Jethro Bakker
 */
public class TopologyNamespaceHandler extends NamespaceHandlerSupport {

	@Override
	public void init() {
		registerBeanDefinitionParser("topology", new TopologyBeanDefinitionParser());
	}

}
