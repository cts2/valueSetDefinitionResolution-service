package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.remoteService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import edu.mayo.cts2.framework.model.command.ResolvedFilter;
import edu.mayo.cts2.framework.model.core.CodeSystemVersionReference;
import edu.mayo.cts2.framework.model.core.ComponentReference;
import edu.mayo.cts2.framework.model.core.MatchAlgorithmReference;
import edu.mayo.cts2.framework.model.entity.EntityDirectoryEntry;
import edu.mayo.cts2.framework.model.valuesetdefinition.ResolvedValueSet;
import edu.mayo.cts2.framework.model.valuesetdefinition.ResolvedValueSetHeader;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.valueSetDefinitionResolutionImpl.ValueSetDefinitionResolutionServiceImpl;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = "classpath:/valueSetDefinitionResolution-service-test-context.xml")
public abstract class ValueSetDefinitionResolutionTestBase
{
	@Autowired public ValueSetDefinitionResolutionServiceImpl vsr;

	protected ResolvedFilter buildFilter(String component, String algorithm, String matchValue)
	{
		ResolvedFilter rf = new ResolvedFilter();
		rf.setMatchValue(matchValue);

		MatchAlgorithmReference mar = new MatchAlgorithmReference(algorithm);
		rf.setMatchAlgorithmReference(mar);

		ComponentReference cr = new ComponentReference();
		cr.setAttributeReference(component);
		rf.setComponentReference(cr);
		return rf;
	}

	protected String[] assemble(String codeSystemVersionURI, String[] codes)
	{
		String[] result = new String[codes.length];
		for (int i = 0; i < codes.length; i++)
		{
			result[i] = codeSystemVersionURI + "/Concept#" + codes[i];
		}
		return result;
	}

	protected void validate(ResolvedValueSet rvs, String[] codes, boolean orderMatters)
	{
		assertEquals("Resolved ValueSet does not contain the expected number of results.", codes.length, rvs.getEntryCount());

		if (orderMatters)
		{
			for (int i = 0; i < codes.length; i++)
			{
				assertEquals("Incorrect code", codes[i], rvs.getEntry(i).getUri());
			}
		}
		else
		{
			HashSet<String> temp = new HashSet<String>(Arrays.asList(codes));

			for (int i = 0; i < codes.length; i++)
			{
				boolean removed = temp.remove(rvs.getEntry(i).getUri());
				assertTrue("Incorrect code, found " + rvs.getEntry(i).getUri() + " which was not expected", removed);
			}
			assertEquals(temp.size(), 0);
		}
	}

	protected void validate(List<EntityDirectoryEntry> entries, String[] codes, boolean orderMatters)
	{
		assertEquals("Resolved ValueSet does not contain the expected number of results.", codes.length, entries.size());

		if (orderMatters)
		{
			for (int i = 0; i < codes.length; i++)
			{
				assertEquals("Incorrect code", codes[i], entries.get(i).getAbout());
			}
		}
		else
		{
			HashSet<String> temp = new HashSet<String>(Arrays.asList(codes));

			for (int i = 0; i < codes.length; i++)
			{
				boolean removed = temp.remove(entries.get(i).getAbout());
				assertTrue("Incorrect code, found " + entries.get(i).getAbout() + " which was not expected", removed);
			}
			assertEquals(temp.size(), 0);
		}
	}

	protected String makeString(String codeSystemName, String codeSystemURI, String codeSystemVersionName, String codeSystemVersionURI)
	{
		return codeSystemName + ":" + codeSystemURI + ":" + codeSystemVersionName + ":" + codeSystemVersionURI;
	}

	protected String makeString(CodeSystemVersionReference csvr)
	{
		return makeString(csvr.getCodeSystem().getContent(), csvr.getCodeSystem().getUri(), csvr.getVersion().getContent(), csvr.getVersion().getUri());
	}

	protected void validate(ResolvedValueSetHeader rvsh, String valueSetName, String valueSetURI, String valueSetDefinitionName, String valueSetDefinitionURI,
			HashSet<String> resolvedUsing, List<String> containsNestedValueSetURI)
	{
		assertEquals(rvsh.getResolutionOf().getValueSet().getUri(), valueSetURI);
		assertEquals(rvsh.getResolutionOf().getValueSet().getContent(), valueSetName);
		assertEquals(rvsh.getResolutionOf().getValueSetDefinition().getUri(), valueSetDefinitionURI);
		if (valueSetDefinitionName != null)
		{
			// Due to multi-threading during load, we can't always predict what the valueSetDefinitionName will be - if null is passed in
			// it is a case where we won't be able to predict.
			assertEquals(rvsh.getResolutionOf().getValueSetDefinition().getContent(), valueSetDefinitionName);
		}

		assertEquals(rvsh.getResolvedUsingCodeSystemCount(), resolvedUsing.size());
		for (CodeSystemVersionReference csvr : rvsh.getResolvedUsingCodeSystemAsReference())
		{
			assertTrue(resolvedUsing.contains(makeString(csvr)));
		}

		assertEquals(rvsh.getIncludesResolvedValueSetCount(), containsNestedValueSetURI.size());
		for (ResolvedValueSetHeader nestedRvsh : rvsh.getIncludesResolvedValueSetAsReference())
		{
			boolean found = false;
			for (String shouldContainNested : containsNestedValueSetURI)
			{
				if (nestedRvsh.getResolutionOf().getValueSetDefinition().getUri().equals(shouldContainNested))
				{
					found = true;
					break;
				}
			}
			if (!found)
			{
				fail("nested IncludedResolvedValueSetAsReference list is missing an item it should have");
			}
		}
	}
}
