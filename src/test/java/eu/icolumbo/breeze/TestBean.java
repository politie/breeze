package eu.icolumbo.breeze;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;


/**
 * @author Pascal S. de Kloe
 */
public class TestBean {

	public void nop() {
	}

	public String none() {
		return null;
	}

	public String ping() {
		return "ping";
	}

	public String echo(String x) {
		return x;
	}

	public String[] array(String a, String b) {
		return new String[] {a, b};
	}

	public List<String> list(String a, String b) {
		return asList(a, b);
	}

	public Map<String,String> map(String a, String b) {
		Map<String,String> result = new HashMap<>();
		result.put("x", a);
		result.put("y", b);
		return result;
	}

}
