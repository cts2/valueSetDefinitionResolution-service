package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices;

import static junit.framework.Assert.assertEquals;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.SetUtilities;

public class UtilitiesTest
{
	@Test
	public void testDestructiveIntersect()
	{
		List<String> a = new ArrayList<String>(Arrays.asList(new String[] { "a", "b", "c" }));
		List<String> b = new ArrayList<String>(Arrays.asList(new String[] { "b", "c" }));

		new SetUtilities<String>().destructiveIntersect(a, b);

		assertEquals(a.size(), 2);
		assertEquals(a.get(0), "b");
		assertEquals(a.get(1), "c");

		a = new ArrayList<String>(Arrays.asList(new String[] { "a", "b", "c" }));
		b = new ArrayList<String>(Arrays.asList(new String[] { "a", "c" }));

		new SetUtilities<String>().destructiveIntersect(a, b);

		assertEquals(a.size(), 2);
		assertEquals(a.get(0), "a");
		assertEquals(a.get(1), "c");

		a = new ArrayList<String>(Arrays.asList(new String[] { "a", "b", "c" }));
		b = new ArrayList<String>(Arrays.asList(new String[] { "a", "b" }));

		new SetUtilities<String>().destructiveIntersect(a, b);

		assertEquals(a.size(), 2);
		assertEquals(a.get(0), "a");
		assertEquals(a.get(1), "b");

		a = new ArrayList<String>(Arrays.asList(new String[] { "a", "b", "c" }));
		b = new ArrayList<String>(Arrays.asList(new String[] {}));

		new SetUtilities<String>().destructiveIntersect(a, b);

		assertEquals(a.size(), 0);

		a = new ArrayList<String>(Arrays.asList(new String[] { "a", "b", "c" }));
		b = new ArrayList<String>(Arrays.asList(new String[] { "a", "b", "c" }));

		new SetUtilities<String>().destructiveIntersect(a, b);

		assertEquals(a.size(), 3);
		assertEquals(a.get(0), "a");
		assertEquals(a.get(1), "b");
		assertEquals(a.get(2), "c");
	}

	@Test
	public void testDestructiveSubtract()
	{
		List<String> a = new ArrayList<String>(Arrays.asList(new String[] { "a", "b", "c" }));
		List<String> b = new ArrayList<String>(Arrays.asList(new String[] { "b", "c" }));

		new SetUtilities<String>().destructiveSubtract(a, b);

		assertEquals(a.size(), 1);
		assertEquals(a.get(0), "a");

		a = new ArrayList<String>(Arrays.asList(new String[] { "a", "b", "c" }));
		b = new ArrayList<String>(Arrays.asList(new String[] { "a", "c" }));

		new SetUtilities<String>().destructiveSubtract(a, b);

		assertEquals(a.size(), 1);
		assertEquals(a.get(0), "b");

		a = new ArrayList<String>(Arrays.asList(new String[] { "a", "b", "c" }));
		b = new ArrayList<String>(Arrays.asList(new String[] { "a", "b" }));

		new SetUtilities<String>().destructiveSubtract(a, b);

		assertEquals(a.size(), 1);
		assertEquals(a.get(0), "c");

		a = new ArrayList<String>(Arrays.asList(new String[] { "a", "b", "c" }));
		b = new ArrayList<String>(Arrays.asList(new String[] {}));

		new SetUtilities<String>().destructiveSubtract(a, b);

		assertEquals(a.size(), 3);
		assertEquals(a.get(0), "a");
		assertEquals(a.get(1), "b");
		assertEquals(a.get(2), "c");

		a = new ArrayList<String>(Arrays.asList(new String[] { "a", "b", "c" }));
		b = new ArrayList<String>(Arrays.asList(new String[] { "a", "b", "c" }));

		new SetUtilities<String>().destructiveSubtract(a, b);

		assertEquals(a.size(), 0);
	}
}
