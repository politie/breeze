package eu.icolumbo.breeze;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;


/**
 * @author Pascal S. de Kloe
 */
public class TestBean {

	public static class Data {
		private String id, message;

		public String getId() {
			return id;
		}

		public void setId(String value) {
			id = value;
		}

		public String getMessage() {
			return message;
		}

		public void setMessage(String value) {
			message = value;
		}

		public void setSetterOnly(Object value) {
		}

		private Object getPrivateGetter() {
			return new Object();
		}
	}

	public void nop() {
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

	public List<Object> list(Object a, Object b) {
		return asList(a, b);
	}

	public Map<String,String> map(String a, String b) {
		Map<String,String> result = new HashMap<>();
		result.put("x", a);
		result.put("y", b);
		return result;
	}

	public Data greet(Number number) {
		Data data = new Data();
		data.setId(number.toString());
		data.setMessage("Hello " + number);
		return data;
	}

	public Data nullObject() {
		return null;
	}

	public Data[] nullArray() {
		return null;
	}

}
