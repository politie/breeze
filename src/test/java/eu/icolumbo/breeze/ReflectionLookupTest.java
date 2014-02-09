package eu.icolumbo.breeze;

import org.junit.Test;
import java.lang.reflect.Method;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


/**
 * Tests {@link FunctionSignature#findMethod(Class)}.
 * @author Pascal S. de Kloe
 */
public class ReflectionLookupTest {

	@Test
	public void methodByArgumentCount() throws Exception {
		Object o = new Object() {
			public void f(int a) {}
			public void f(int a, int b) {}
			public void f(int a, int b, int c) {}
		};

		Method match = findMethod(o.getClass(), "f", 2);
		assertArrayEquals(new Class<?>[] { int.class, int.class }, match.getParameterTypes());
	}

	@Test
	public void methodRefinement() throws Exception {
		Object o = new Object() {
			public void f(String x) {}
			public void f(Object x) {}
			public void f(CharSequence x) {}
		};

		Method match = findMethod(o.getClass(), "f", 1);
		assertArrayEquals(new Class<?>[]{Object.class}, match.getParameterTypes());
	}

	@Test
	public void methodAmbiguity() throws Exception {
		Object o = new Object() {
			public void f(int x) {}
			public void f(float y) {}
		};

		try {
			findMethod(o.getClass(), "f", 1);
			fail("no exception");
		} catch (ReflectiveOperationException e) {
			assertEquals("Ambiguity between public void " +
					"eu.icolumbo.breeze.ReflectionLookupTest$3.f(float)" +
					" and public void " +
					"eu.icolumbo.breeze.ReflectionLookupTest$3.f(int)",
					e.getMessage());
		}
	}

	@Test
	public void methodOverride() throws Exception {
		class Super {
			public Number id(long i) { return i; }
		}
		class Sub extends Super {
			@Override public Number id(long i) { return i * 2.0; }
		}

		assertNotNull(findMethod(Sub.class, "hashCode", 0));
		assertNotNull(findMethod(Sub.class, "equals", 1));

		Method superMethod = findMethod(Super.class, "id", 1);
		Method subMethod = findMethod(Sub.class, "id", 1);
		assertEquals(42L, superMethod.invoke(new Super(), 42));
		assertEquals(84d, superMethod.invoke(new Sub(), 42));
		assertEquals(84d, subMethod.invoke(new Sub(), 42));
	}

	interface IntegerArithmetic {
		long add(long a, long b);
	}
	interface RealArithmetic {
		double add(double a, double b);
		double sqrt(double x);
	}
	interface Arithmetic extends IntegerArithmetic, RealArithmetic {
	}

	@Test
	public void interfaceMethodInheritance() throws Exception {
		assertNotNull(findMethod(RealArithmetic.class, "sqrt", 1));
		assertNotNull(findMethod(Arithmetic.class, "sqrt", 1));
	}

	@Test
	public void interfaceMethodNotFound() throws Exception {
		try {
			findMethod(IntegerArithmetic.class, "sqrt", 1);
			fail("no exception");
		} catch (NoSuchMethodException e) {
			assertEquals("No method interface " +
					"eu.icolumbo.breeze.ReflectionLookupTest$IntegerArithmetic#sqrt" +
					" with 1 parameters", e.getMessage());
		}

	}

	@Test
	public void interfaceMethodAmbiguity() throws Exception {
		try {
			findMethod(Arithmetic.class, "add", 2);
			fail("no exception");
		} catch (ReflectiveOperationException e) {
			assertEquals("Ambiguity between public abstract double " +
					"eu.icolumbo.breeze.ReflectionLookupTest$RealArithmetic.add(double,double)" +
					" and public abstract long " +
					"eu.icolumbo.breeze.ReflectionLookupTest$IntegerArithmetic.add(long,long)", e.getMessage());
		}
	}

	@Test
	public void methodByInterface() throws Exception {
		Object o = new Arithmetic() {
			@Override public long add(long a, long b) { return a + b; }
			@Override public double add(double a, double b) { return a + b; }
			@Override public double sqrt(double x) { return Math.sqrt(x); }
		};
		Method i = findMethod(IntegerArithmetic.class, "add", 2);
		Method r = findMethod(RealArithmetic.class, "add", 2);
		assertEquals(3L, i.invoke(o, 1, 2));
		assertEquals(3d, r.invoke(o, 1, 2));
	}

	@Test
	public void generics() throws Exception {
		Object o = new Comparable<String>() {
			@Override public int compareTo(String o) { return 1; }
		};
		Method m = findMethod(Comparable.class, "compareTo", 1);
		assertArrayEquals(new Class<?>[] { Object.class }, m.getParameterTypes());
		Method n = findMethod(o.getClass(), "compareTo", 1);
		assertArrayEquals(new Class<?>[]{ Object.class }, n.getParameterTypes());
	}

	private static Method findMethod(Class<?> type, String method, int paramCount)
	throws Exception{
		String[] params = new String[paramCount];
		Arrays.fill(params, "arbitrary");
		return new FunctionSignature(method, params).findMethod(type);
	}

}
