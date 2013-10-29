package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.remoteService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.junit.Ignore;
import org.junit.Test;
import edu.mayo.cts2.framework.model.command.Page;
import edu.mayo.cts2.framework.model.core.ComponentReference;
import edu.mayo.cts2.framework.model.core.DescriptionInCodeSystem;
import edu.mayo.cts2.framework.model.core.ScopedEntityName;
import edu.mayo.cts2.framework.model.core.SortCriteria;
import edu.mayo.cts2.framework.model.core.SortCriterion;
import edu.mayo.cts2.framework.model.core.types.SetOperator;
import edu.mayo.cts2.framework.model.core.types.SortDirection;
import edu.mayo.cts2.framework.model.entity.EntityDirectoryEntry;
import edu.mayo.cts2.framework.model.service.core.EntityNameOrURI;
import edu.mayo.cts2.framework.model.service.core.NameOrURI;
import edu.mayo.cts2.framework.model.service.core.NameOrURIList;
import edu.mayo.cts2.framework.model.service.core.Query;
import edu.mayo.cts2.framework.model.service.core.Query6Choice;
import edu.mayo.cts2.framework.model.service.core.Query6Choice2;
import edu.mayo.cts2.framework.model.service.exception.UnknownCodeSystemVersion;
import edu.mayo.cts2.framework.model.valuesetdefinition.ResolvedValueSet;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ctsUtility.queryBuilders.ResolvedValueSetResolutionEntityQueryBuilder;
import edu.mayo.cts2.framework.service.constant.ExternalCts2Constants;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ResolvedValueSetResolutionEntityQuery;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ResolvedValueSetResult;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.name.ValueSetDefinitionReadId;

/**
 * This set of tests is a strange one.  They are purposefully set to ignore, because at the moment, 
 * they cannot be run in an automated fashion.
 * 
 * These tests were initially written as part of the exist-service - and when they run there, they 
 * do run in an automated mode - they load the data into exist, and all execute.  However - the 
 * valueSetDefinitionResolution service is running in local-service mode at that time - all cts
 * queries are passed directly to the local service instances.
 * 
 * This valueSetDefinitionResolution implementation also fully supports running in remote mode - where 
 * the only access to code systems, value sets, etc - is via REST calls to a server configured in 
 * valueSetDefinition_config.properties.
 * 
 * In order to run all of the tests in remote mode - we need a remote exist server.  But we can't depend
 * on the exist server  (and use that for automated tests) because the exist server depends on this code.
 * 
 * In the end, I finally just decided that this would be a manual test for now, as I ran out of cleverness
 * and time.
 * 
 *  Steps to run:
 *  
 *  - Remove the Ignore tag.
 *  - Load up a cts2 server with the data specified in src/test/resources/valuesetResolutionData
 *    - load all 3 files
 *    - these files are identical to the data files found in the exist-service implementation.
 *    - A handy way to get the data loaded is to simply run the test 
 *      {exist-service}/src/main/test/java/edu.mayo.cts2.framework.plugin.service.exist.valuesetResolution.ValueSetDefinitionResolutionTest
 *      That test loads the exist server as part of its test cycle, and does not delete the DB afterword (you can pick up the exist database 
 *      out of your temp folder) - and it uses the exact same data as this set of tests (since they are the same tests)
 *  
 *  - Configure src/test/resources/valueSetDefinition_config-test.properties (if necessary) to point to the location where the cts server lives.
 *  - Configure 'rootURL' (just below) the same way (if necessary)
 *  - Run this test class.
 *  
 *    Yes, these tests are complete copy/paste from the tests in the exist-service with a couple of very minor changes to the tests that use the
 *    rootURL parameter.  They were restructured into a single class here, however, instead of two for simplicity (two were not required here - 
 *    they are in exist because some run in a local test env, but others require the integration environment to be up)
 *    
 *    The data for these tests is exactly the same as the data for the exist-service tests - it exists in both projects (although it is not referenced
 *    programmatically in this package)
 *    
 *    What should probably happen if any future changes need to be made to these tests is to create a new stand-alone valueSetDefinitionResolutionTestPackage
 *    then both the valueSetDefinitionResolution-service and the exist-service could depend on it.  
 */

@Ignore
public class ValueSetDefinitionResolutionTest extends ValueSetDefinitionResolutionTestBase
{	
	static final String rootURL = "http://localhost:8080/";
	
	@Test
	public void testCompleteCodeSystem()
	{
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet1-1.0"), null, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData", new String[] {"A", "B", "C", "D"}), false);
		
		HashSet<String> resolvedUsing = new HashSet<String>();
		resolvedUsing.add(makeString("UnbelievableTestData", "tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData", "UnbelievableTestData-1.0", 
				"tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData-1.0"));
		
		validate(rvs.getResolutionInfo(), "ValueSet1", "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet1", null, "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet1-1.0",
				resolvedUsing, new ArrayList<String>());
	}
	
	@Test
	public void testTransitiveClosure()
	{
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet2-1.0"), null, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData", new String[] {"A", "B", "C", "D"}), false);
	}
	
	@Test
	public void testTransitiveClosure2()
	{
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet5-1.0"), null, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:BelievableTestData", new String[] {"1", "2", "3", "4"}), false);
	}
	
	@Test
	public void testTransitiveClosure3()
	{
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet5-2.0"), null, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:BelievableTestData", new String[] {"1", "2", "3"}), false);
	}
	
	@Test
	public void testCycleDetect()
	{
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet5-3.0"), null, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:BelievableTestData", new String[] {"6", "5"}), false);
	}
	
	@Test
	public void testCycleDetect2()
	{
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet5-7.0"), null, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:BelievableTestData", new String[] {}), false);
	}
	
	@Test
	public void testLeafOnly()
	{
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet5-4.0"), null, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:BelievableTestData", new String[] {"1"}), false);
	}
	
	@Test
	public void testLeafOnlyDirect()
	{
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet5-5.0"), null, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:BelievableTestData", new String[] {"3"}), false);
	}
	
	@Test
	public void testLeafOnlyAll()
	{
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet5-6.0"), null, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:BelievableTestData", new String[] {"3"}), false);
	}
	
	
	@Test
	public void testEntityList()
	{
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet3-1.0"), null, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData", new String[] {"A", "C"}), false);
	}
	
	@Test
	public void testSubtractAndCompleteValueSet()
	{
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet4-1.0"), null, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData", new String[] {"A", "B", "D"}), false);
	}
	
	@Test
	public void testSort1()
	{
		SortCriteria sc = new SortCriteria();
		SortCriterion sc1 = new SortCriterion();
		sc1.setEntryOrder(1l);
		sc1.setSortDirection(SortDirection.ASCENDING);
		ComponentReference cr = new ComponentReference();
		cr.setAttributeReference(ExternalCts2Constants.MA_ENTITY_DESCRIPTION_DESIGNATION_NAME);
		sc1.setSortElement(cr);
		sc.addEntry(sc1);
		SortCriterion sc2 = new SortCriterion();
		sc2.setEntryOrder(2l);
		sc2.setSortDirection(SortDirection.DESCENDING);
		cr = new ComponentReference();
		cr.setAttributeReference(ExternalCts2Constants.MA_RESOURCE_NAME_NAME);
		sc2.setSortElement(cr);
		sc.addEntry(sc2);
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet6-1.0"), null, null, sc, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:BelievableTestData", new String[] {"2", "1", "3", "4", "5", "6", "11", "12", "111"}), true);
	}
	
	@Test
	public void testSort2()
	{
		SortCriteria sc = new SortCriteria();
		SortCriterion sc2 = new SortCriterion();
		sc2.setEntryOrder(2l);
		sc2.setSortDirection(SortDirection.ASCENDING);
		ComponentReference cr = new ComponentReference();
		cr.setAttributeReference(ExternalCts2Constants.MA_RESOURCE_NAME_NAME);
		sc2.setSortElement(cr);
		sc.addEntry(sc2);
		
		//purposefully loading them backwards of the entry order to catch out trusting implementations....
		SortCriterion sc1 = new SortCriterion();
		sc1.setEntryOrder(1l);
		sc1.setSortDirection(SortDirection.DESCENDING);
		cr = new ComponentReference();
		cr.setAttributeReference(ExternalCts2Constants.MA_ENTITY_DESCRIPTION_DESIGNATION_NAME);
		sc1.setSortElement(cr);
		sc.addEntry(sc1);
		
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet6-1.0"), null, null, sc, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:BelievableTestData", new String[] {"111", "12", "11", "6", "5", "4", "3", "1","2"}), true);
	}
	
	@Test
	public void testSort3()
	{
		SortCriteria sc = new SortCriteria();
		SortCriterion sc1 = new SortCriterion();
		sc1.setEntryOrder(1l);
		sc1.setSortDirection(SortDirection.DESCENDING);
		ComponentReference cr = new ComponentReference();
		cr.setAttributeReference(ExternalCts2Constants.MA_RESOURCE_NAME_NAME);
		sc1.setSortElement(cr);
		sc.addEntry(sc1);
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet6-1.0"), null, null, sc, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:BelievableTestData", new String[] {"111", "12", "11", "6", "5", "4", "3", "2", "1"}), true);
	}
	
	@Test
	public void testSort4()
	{
		SortCriteria sc = new SortCriteria();
		SortCriterion sc1 = new SortCriterion();
		sc1.setEntryOrder(1l);
		sc1.setSortDirection(SortDirection.ASCENDING);
		ComponentReference cr = new ComponentReference();
		cr.setAttributeReference(ExternalCts2Constants.MA_RESOURCE_NAME_NAME);
		sc1.setSortElement(cr);
		sc.addEntry(sc1);
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet6-1.0"), null, null, sc, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:BelievableTestData", new String[] {"1", "2", "3", "4", "5", "6", "11", "12", "111"}), true);
	}
	
	@Test
	public void testPropertyQueryRef1()
	{
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet7-1.0"), null, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData", new String[] {"B"}), false);
	}
	
	@Test
	public void testPropertyQueryRef2()
	{
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet7-2.0"), null, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData", new String[] {"A", "B"}), false);
	}
	
	@Test
	public void testHeaderAcrossTwo()
	{
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet8-1.0"), null, null, null, null);
		validate(rvs, new String[] {"tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData/Concept#A", 
				"tag:informatics.mayo.edu,2013-08-03:BelievableTestData/Concept#1"}, false);
		
		HashSet<String> resolvedUsing = new HashSet<String>();
		resolvedUsing.add(makeString("UnbelievableTestData", "tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData", "UnbelievableTestData-1.0", 
				"tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData-1.0"));
		resolvedUsing.add(makeString("BelievableTestData", "tag:informatics.mayo.edu,2013-08-03:BelievableTestData", "BelievableTestData-1.0", 
				"tag:informatics.mayo.edu,2013-08-03:BelievableTestData-1.0"));

		
		validate(rvs.getResolutionInfo(), "ValueSet8", "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet8", null, "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet8-1.0",
				resolvedUsing, new ArrayList<String>());
	}
	
	@Test
	public void testVersionResolutionBadVer1()
	{
		Set<NameOrURI> shouldResolveUsing = new HashSet<NameOrURI>();
		NameOrURI nou = new NameOrURI();
		nou.setUri("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-5.0");
		shouldResolveUsing.add(nou);
		
		try
		{
			vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-1.0"), 
				shouldResolveUsing, null, null, null);
			fail("didn't validate requested code system version list");
		}
		catch (UnknownCodeSystemVersion e)
		{
			//expected
		}
	}
	
	@Test
	public void testVersionResolutionBadVer2()
	{
		Set<NameOrURI> shouldResolveUsing = new HashSet<NameOrURI>();
		NameOrURI nou = new NameOrURI();
		nou.setUri("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-1.0");
		shouldResolveUsing.add(nou);
		nou = new NameOrURI();
		nou.setUri("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-2.0");
		shouldResolveUsing.add(nou);
		
		try
		{
			vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-1.0"), 
				shouldResolveUsing, null, null, null);
			fail("didn't validate 1 version per code system");
		}
		catch (Exception e)
		{
			//expected
		}
	}
	
	@Test
	public void testVersionResolution1()
	{
		Set<NameOrURI> shouldResolveUsing = new HashSet<NameOrURI>();
		NameOrURI nou = new NameOrURI();
		nou.setUri("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-1.0");
		shouldResolveUsing.add(nou);
		
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-1.0"), 
				shouldResolveUsing, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", new String[] {"157", "42"}), false);
		
		HashSet<String> resolvedUsing = new HashSet<String>();
		resolvedUsing.add(makeString("MultiVersionTestData", "tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", "MultiVersionTestData-1.0", 
				"tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-1.0"));
		
		validate(rvs.getResolutionInfo(), "ValueSet9", "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9", null, "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-1.0",
				resolvedUsing, new ArrayList<String>());
	}
	
	@Test
	public void testVersionResolution2()
	{
		Set<NameOrURI> shouldResolveUsing = new HashSet<NameOrURI>();
		NameOrURI nou = new NameOrURI();
		nou.setUri("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-2.0");
		shouldResolveUsing.add(nou);
		
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-1.0"), 
				shouldResolveUsing, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", new String[] {"257"}), false);
		
		HashSet<String> resolvedUsing = new HashSet<String>();
		resolvedUsing.add(makeString("MultiVersionTestData", "tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", "MultiVersionTestData-2.0", 
				"tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-2.0"));
		
		validate(rvs.getResolutionInfo(), "ValueSet9", "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9", null, "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-1.0",
				resolvedUsing, new ArrayList<String>());
	}
	
	@Test
	public void testVersionResolution3()
	{
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-2.0"), 
				null, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", new String[] {"157", "42"}), false);
		
		HashSet<String> resolvedUsing = new HashSet<String>();
		resolvedUsing.add(makeString("MultiVersionTestData", "tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", "MultiVersionTestData-1.0", 
				"tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-1.0"));
		
		validate(rvs.getResolutionInfo(), "ValueSet9", "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9", null, "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-2.0",
				resolvedUsing, new ArrayList<String>());
	}
	
	@Test
	public void testVersionResolution4()
	{
		//this should be ignored
		Set<NameOrURI> shouldResolveUsing = new HashSet<NameOrURI>();
		NameOrURI nou = new NameOrURI();
		nou.setUri("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-2.0");
		shouldResolveUsing.add(nou);
		
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-2.0"), 
				shouldResolveUsing, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", new String[] {"157", "42"}), false);
		
		HashSet<String> resolvedUsing = new HashSet<String>();
		resolvedUsing.add(makeString("MultiVersionTestData", "tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", "MultiVersionTestData-1.0", 
				"tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-1.0"));
		
		validate(rvs.getResolutionInfo(), "ValueSet9", "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9", null, "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-2.0",
				resolvedUsing, new ArrayList<String>());
	}
	
	@Test
	public void testVersionResolution5()
	{
		//this should be ignored
		Set<NameOrURI> shouldResolveUsing = new HashSet<NameOrURI>();
		NameOrURI nou = new NameOrURI();
		nou.setUri("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-1.0");
		shouldResolveUsing.add(nou);
		
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-3.0"), 
				shouldResolveUsing, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", new String[] {"257"}), false);
		
		HashSet<String> resolvedUsing = new HashSet<String>();
		resolvedUsing.add(makeString("MultiVersionTestData", "tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", "MultiVersionTestData-2.0", 
				"tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-2.0"));
		
		validate(rvs.getResolutionInfo(), "ValueSet9", "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9", null, "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-3.0",
				resolvedUsing, new ArrayList<String>());
	}
	
	@Test
	public void testVersionResolution6()
	{
		Set<NameOrURI> shouldResolveUsing = new HashSet<NameOrURI>();
		NameOrURI nou = new NameOrURI();
		nou.setUri("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-1.0");
		shouldResolveUsing.add(nou);
		
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-4.0"), 
				shouldResolveUsing, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", new String[] {"157"}), false);
		
		assertEquals("something in version 1",rvs.getEntry(0).getDesignation());
		
		HashSet<String> resolvedUsing = new HashSet<String>();
		resolvedUsing.add(makeString("MultiVersionTestData", "tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", "MultiVersionTestData-1.0", 
				"tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-1.0"));
		
		validate(rvs.getResolutionInfo(), "ValueSet9", "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9", null, "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-4.0",
				resolvedUsing, new ArrayList<String>());
	}
	
	@Test
	public void testVersionResolution7()
	{
		Set<NameOrURI> shouldResolveUsing = new HashSet<NameOrURI>();
		NameOrURI nou = new NameOrURI();
		nou.setUri("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-3.0");
		shouldResolveUsing.add(nou);
		
		ResolvedValueSet rvs = vsr.resolveDefinitionAsCompleteSet(new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-4.0"), 
				shouldResolveUsing, null, null, null);
		validate(rvs, assemble("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", new String[] {"157"}), false);
		
		assertEquals("same concept redefined with a different desc in ver 3",rvs.getEntry(0).getDesignation());
		
		HashSet<String> resolvedUsing = new HashSet<String>();
		resolvedUsing.add(makeString("MultiVersionTestData", "tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", "MultiVersionTestData-3.0", 
				"tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-3.0"));
		
		validate(rvs.getResolutionInfo(), "ValueSet9", "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9", null, "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-4.0",
				resolvedUsing, new ArrayList<String>());
	}
	
	@Test
	public void testVersionResolution8()
	{
		Set<NameOrURI> shouldResolveUsing = new HashSet<NameOrURI>();
		NameOrURI nou = new NameOrURI();
		nou.setUri("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-1.0");
		shouldResolveUsing.add(nou);
		
		Page p = new Page();
		p.setMaxToReturn(50);
		p.setPage(0);
		
		ResolvedValueSetResult<EntityDirectoryEntry> rvsr = vsr.resolveDefinitionAsEntityDirectory(
				new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-5.0"), 
				shouldResolveUsing, null, null, null, null, p);
		
		validate(rvsr.getEntries(), assemble("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", new String[] {"157", "42"}), false);
		
		DescriptionInCodeSystem[] d = (rvsr.getEntries().get(0).getResourceName().equals("157") ? 
				rvsr.getEntries().get(0).getKnownEntityDescription() : rvsr.getEntries().get(1).getKnownEntityDescription()); 
		
		assertEquals("something in version 1",d[0].getDesignation());
		assertEquals(1, d.length);
		
		HashSet<String> resolvedUsing = new HashSet<String>();
		resolvedUsing.add(makeString("MultiVersionTestData", "tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", "MultiVersionTestData-1.0", 
				"tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-1.0"));
		
		validate(rvsr.getResolvedValueSetHeader(), "ValueSet9", "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9", null, "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-5.0",
				resolvedUsing, new ArrayList<String>());
	}
	
	@Test
	public void testVersionResolution9()
	{
		Set<NameOrURI> shouldResolveUsing = new HashSet<NameOrURI>();
		NameOrURI nou = new NameOrURI();
		nou.setUri("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-3.0");
		shouldResolveUsing.add(nou);
		
		Page p = new Page();
		p.setMaxToReturn(50);
		p.setPage(0);
		
		ResolvedValueSetResult<EntityDirectoryEntry> rvsr = vsr.resolveDefinitionAsEntityDirectory(
				new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-5.0"), 
				shouldResolveUsing, null, null, null, null, p);
		validate(rvsr.getEntries(), assemble("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", new String[] {"157", "42"}), false);
		
		DescriptionInCodeSystem[] d = (rvsr.getEntries().get(0).getResourceName().equals("157") ? 
				rvsr.getEntries().get(0).getKnownEntityDescription() : rvsr.getEntries().get(1).getKnownEntityDescription()); 
		
		assertEquals("same concept redefined with a different desc in ver 3",d[0].getDesignation());
		assertEquals(1, d.length);
		
		HashSet<String> resolvedUsing = new HashSet<String>();
		resolvedUsing.add(makeString("MultiVersionTestData", "tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", "MultiVersionTestData-3.0", 
				"tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-3.0"));
		
		validate(rvsr.getResolvedValueSetHeader(), "ValueSet9", "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9", null, "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-5.0",
				resolvedUsing, new ArrayList<String>());
	}
	
	//same test as 8, but this time, using an association to find entities
	@Test
	public void testVersionResolution8_1()
	{
		Set<NameOrURI> shouldResolveUsing = new HashSet<NameOrURI>();
		NameOrURI nou = new NameOrURI();
		nou.setUri("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-1.0");
		shouldResolveUsing.add(nou);
		
		Page p = new Page();
		p.setMaxToReturn(50);
		p.setPage(0);
		
		ResolvedValueSetResult<EntityDirectoryEntry> rvsr = vsr.resolveDefinitionAsEntityDirectory(
				new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-6.0"), 
				shouldResolveUsing, null, null, null, null, p);
		
		validate(rvsr.getEntries(), assemble("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", new String[] {"157"}), false);
		
		DescriptionInCodeSystem[] d = (rvsr.getEntries().get(0).getResourceName().equals("157") ? 
				rvsr.getEntries().get(0).getKnownEntityDescription() : rvsr.getEntries().get(1).getKnownEntityDescription()); 
		
		assertEquals("something in version 1",d[0].getDesignation());
		assertEquals(1, d.length);
		
		HashSet<String> resolvedUsing = new HashSet<String>();
		resolvedUsing.add(makeString("MultiVersionTestData", "tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", "MultiVersionTestData-1.0", 
				"tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-1.0"));
		
		validate(rvsr.getResolvedValueSetHeader(), "ValueSet9", "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9", null, "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-6.0",
				resolvedUsing, new ArrayList<String>());
	}
	
	//same test as 9, but this time, using an association to find entities
	@Test
	public void testVersionResolution9_1()
	{
		Set<NameOrURI> shouldResolveUsing = new HashSet<NameOrURI>();
		NameOrURI nou = new NameOrURI();
		nou.setUri("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-3.0");
		shouldResolveUsing.add(nou);
		
		Page p = new Page();
		p.setMaxToReturn(50);
		p.setPage(0);
		
		ResolvedValueSetResult<EntityDirectoryEntry> rvsr = vsr.resolveDefinitionAsEntityDirectory(
				new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-6.0"), 
				shouldResolveUsing, null, null, null, null, p);
		validate(rvsr.getEntries(), assemble("tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", new String[] {"157"}), false);
		
		DescriptionInCodeSystem[] d = (rvsr.getEntries().get(0).getResourceName().equals("157") ? 
				rvsr.getEntries().get(0).getKnownEntityDescription() : rvsr.getEntries().get(1).getKnownEntityDescription()); 
		
		assertEquals("same concept redefined with a different desc in ver 3",d[0].getDesignation());
		assertEquals(1, d.length);
		
		HashSet<String> resolvedUsing = new HashSet<String>();
		resolvedUsing.add(makeString("MultiVersionTestData", "tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData", "MultiVersionTestData-3.0", 
				"tag:informatics.mayo.edu,2013-08-03:MultiVersionTestData-3.0"));
		
		validate(rvsr.getResolvedValueSetHeader(), "ValueSet9", "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9", null, "tag:informatics.mayo.edu,2013-08-03:vs/ValueSet9-6.0",
				resolvedUsing, new ArrayList<String>());
	}
	
	@Test
	public void queryTestFiltering1()
	{
		ResolvedValueSetResolutionEntityQuery query = ResolvedValueSetResolutionEntityQueryBuilder.build();
		query.getFilterComponent().add(buildFilter(ExternalCts2Constants.MA_RESOURCE_NAME_NAME, "exactMatch", "3"));
		
		ResolvedValueSetResult<EntityDirectoryEntry> rvsr = vsr.resolveDefinitionAsEntityDirectory(
				new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet6-1.0"), 
				null, null, query, null, null, new Page());
		
		validate(rvsr.getEntries(), assemble("tag:informatics.mayo.edu,2013-08-03:BelievableTestData", new String[] {"3"}), false);
	}
	
	@Test
	public void queryTestFiltering2()
	{
		ResolvedValueSetResolutionEntityQuery query = ResolvedValueSetResolutionEntityQueryBuilder.build();
		query.getFilterComponent().add(buildFilter(ExternalCts2Constants.MA_ENTITY_DESCRIPTION_DESIGNATION_NAME, "contains", "number"));
		
		ResolvedValueSetResult<EntityDirectoryEntry> rvsr = vsr.resolveDefinitionAsEntityDirectory(
				new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet6-1.0"), 
				null, null, query, null, null, new Page());
		
		validate(rvsr.getEntries(), assemble("tag:informatics.mayo.edu,2013-08-03:BelievableTestData", new String[] {"12", "111"}), false);
	}
	
	@Test
	public void queryTestFiltering3()
	{
		ResolvedValueSetResolutionEntityQuery query = ResolvedValueSetResolutionEntityQueryBuilder.build();
		query.getFilterComponent().add(buildFilter(ExternalCts2Constants.MA_ENTITY_DESCRIPTION_DESIGNATION_NAME, "startsWith", "a"));
		
		ResolvedValueSetResult<EntityDirectoryEntry> rvsr = vsr.resolveDefinitionAsEntityDirectory(
				new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet6-1.0"), 
				null, null, query, null, null, new Page());
		
		validate(rvsr.getEntries(), assemble("tag:informatics.mayo.edu,2013-08-03:BelievableTestData", new String[] {"1", "2"}), false);
	}
	
	@Test
	public void queryTestEntityRestrictions()
	{
		ResolvedValueSetResolutionEntityQuery query = ResolvedValueSetResolutionEntityQueryBuilder.build();
		EntityNameOrURI en = new EntityNameOrURI();
		ScopedEntityName sen = new ScopedEntityName();
		sen.setName("2");
		sen.setNamespace("BelievableTestData");
		en.setEntityName(sen);
		query.getResolvedValueSetResolutionEntityRestrictions().getEntities().add(en);
		en = new EntityNameOrURI();
		en.setUri("tag:informatics.mayo.edu,2013-08-03:BelievableTestData/Concept#11");
		query.getResolvedValueSetResolutionEntityRestrictions().getEntities().add(en);
		
		ResolvedValueSetResult<EntityDirectoryEntry> rvsr = vsr.resolveDefinitionAsEntityDirectory(
				new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet6-1.0"), 
				null, null, query, null, null, new Page());
		
		validate(rvsr.getEntries(), assemble("tag:informatics.mayo.edu,2013-08-03:BelievableTestData", new String[] {"2", "11"}), false);
	}
	
	@Test
	public void queryTestEntityRestrictions2()
	{
		ResolvedValueSetResolutionEntityQuery query = ResolvedValueSetResolutionEntityQueryBuilder.build();

		NameOrURI nou = new NameOrURI();
		nou.setName("UnbelievableTestData-1.0");
		query.getResolvedValueSetResolutionEntityRestrictions().setCodeSystemVersion(nou);
		
		ResolvedValueSetResult<EntityDirectoryEntry> rvsr = vsr.resolveDefinitionAsEntityDirectory(
				new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet8-1.0"), 
				null, null, query, null, null, new Page());
		
		validate(rvsr.getEntries(), assemble("tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData", new String[] {"A"}), false);
	}
	
	@Test
	public void queryTestEntityRestrictions3()
	{
		ResolvedValueSetResolutionEntityQuery query = ResolvedValueSetResolutionEntityQueryBuilder.build();

		NameOrURI nou = new NameOrURI();
		nou.setUri("tag:informatics.mayo.edu,2013-08-03:BelievableTestData-1.0");
		query.getResolvedValueSetResolutionEntityRestrictions().setCodeSystemVersion(nou);
		
		ResolvedValueSetResult<EntityDirectoryEntry> rvsr = vsr.resolveDefinitionAsEntityDirectory(
				new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet8-1.0"), 
				null, null, query, null, null, new Page());
		
		validate(rvsr.getEntries(), assemble("tag:informatics.mayo.edu,2013-08-03:BelievableTestData", new String[] {"1"}), false);
	}
	
	@Test
	public void queryTestSubQueries()
	{
		ResolvedValueSetResolutionEntityQuery query = ResolvedValueSetResolutionEntityQueryBuilder.build();

		Query6Choice subQuery = new Query6Choice();
		subQuery.setDirectoryUri1(rootURL + "valueset/ValueSet3/definition/1/entities");
		
		query.getQuery().setQuery6Choice(subQuery);
		query.getQuery().setSetOperation(SetOperator.UNION);
		
		ResolvedValueSetResult<EntityDirectoryEntry> rvsr = vsr.resolveDefinitionAsEntityDirectory(
				new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet8-1.0"), 
				null, null, query, null, null, new Page());
		
		validate(rvsr.getEntries(), new String[] {"tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData/Concept#A", 
			"tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData/Concept#C",
			"tag:informatics.mayo.edu,2013-08-03:BelievableTestData/Concept#1"}, false);
	}
	
	@Test
	public void queryTestSubQueries2()
	{
		Query nestedQuery = new Query();
		
		Query6Choice nestedQueryLeft = new Query6Choice();
		nestedQueryLeft.setDirectoryUri1(rootURL + "codesystem/UnbelievableTestData/version/UnbelievableTestData-1.0/entities");
		nestedQuery.setQuery6Choice(nestedQueryLeft);
		
		Query6Choice2 nestedQueryRight = new Query6Choice2();
		nestedQueryRight.setDirectoryUri2(rootURL + "valueset/ValueSet4/definition/1/entities");
		nestedQuery.setQuery6Choice2(nestedQueryRight);

		nestedQuery.setSetOperation(SetOperator.SUBTRACT);
			
		
		ResolvedValueSetResolutionEntityQuery query = ResolvedValueSetResolutionEntityQueryBuilder.build();
		Query6Choice topQueryRight = new Query6Choice();
		topQueryRight.setQuery1(nestedQuery);
		query.getQuery().setQuery6Choice(topQueryRight);
		query.getQuery().setSetOperation(SetOperator.INTERSECT);
		
		ResolvedValueSetResult<EntityDirectoryEntry> rvsr = vsr.resolveDefinitionAsEntityDirectory(
				new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet1-1.0"), 
				null, null, query, null, null, new Page());
		
		validate(rvsr.getEntries(), assemble("tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData", new String[] {"C"}), false);
	}
	
	@Test
	public void queryTestSubQueries3()
	{
		Query nestedQuery = new Query();
		
		Query6Choice nestedQueryLeft = new Query6Choice();
		nestedQueryLeft.setDirectoryUri1(rootURL + "codesystem/UnbelievableTestData/version/UnbelievableTestData-1.0/entities");
		nestedQuery.setQuery6Choice(nestedQueryLeft);
		
		Query6Choice2 nestedQueryRight = new Query6Choice2();
		nestedQueryRight.setDirectoryUri2(rootURL + "codesystem/UnbelievableTestData/version/BelievableTestData-1.0/entities");
		nestedQuery.setQuery6Choice2(nestedQueryRight);
		
		nestedQuery.setSetOperation(SetOperator.UNION);
		
		NameOrURIList filters = new NameOrURIList();
		NameOrURI nou = new NameOrURI();
		nou.setName(ExternalCts2Constants.MA_ENTITY_DESCRIPTION_DESIGNATION_NAME);
		filters.addEntry(nou);
		nestedQuery.setFilterComponent(filters);
		NameOrURI matchAlgorithm = new NameOrURI();
		matchAlgorithm.setName("contains");
		nestedQuery.setMatchAlgorithm(matchAlgorithm);
		nestedQuery.setMatchValue("filter");
		
		ResolvedValueSetResolutionEntityQuery query = ResolvedValueSetResolutionEntityQueryBuilder.build();
		Query6Choice topQueryRight = new Query6Choice();
		topQueryRight.setQuery1(nestedQuery);
		query.getQuery().setQuery6Choice(topQueryRight);
		query.getQuery().setSetOperation(SetOperator.UNION);
		
		ResolvedValueSetResult<EntityDirectoryEntry> rvsr = vsr.resolveDefinitionAsEntityDirectory(
				new ValueSetDefinitionReadId("tag:informatics.mayo.edu,2013-08-03:vs/ValueSet3-1.0"), 
				null, null, query, null, null, new Page());
		
		validate(rvsr.getEntries(), new String[] {"tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData/Concept#A", 
			"tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData/Concept#B", 
			"tag:informatics.mayo.edu,2013-08-03:UnbelievableTestData/Concept#C",
			"tag:informatics.mayo.edu,2013-08-03:BelievableTestData/Concept#6"}, false);
	}
}
