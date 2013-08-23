package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.valueSetDefinitionResolution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import edu.mayo.cts2.framework.core.client.Cts2RestClient;
import edu.mayo.cts2.framework.model.association.AssociationDirectory;
import edu.mayo.cts2.framework.model.association.AssociationDirectoryEntry;
import edu.mayo.cts2.framework.model.codesystemversion.CodeSystemVersionCatalogEntry;
import edu.mayo.cts2.framework.model.command.Page;
import edu.mayo.cts2.framework.model.command.ResolvedFilter;
import edu.mayo.cts2.framework.model.command.ResolvedReadContext;
import edu.mayo.cts2.framework.model.core.CodeSystemVersionReference;
import edu.mayo.cts2.framework.model.core.ComponentReference;
import edu.mayo.cts2.framework.model.core.DescriptionInCodeSystem;
import edu.mayo.cts2.framework.model.core.EntityReference;
import edu.mayo.cts2.framework.model.core.MatchAlgorithmReference;
import edu.mayo.cts2.framework.model.core.NameAndMeaningReference;
import edu.mayo.cts2.framework.model.core.PredicateReference;
import edu.mayo.cts2.framework.model.core.ScopedEntityName;
import edu.mayo.cts2.framework.model.core.SortCriteria;
import edu.mayo.cts2.framework.model.core.URIAndEntityName;
import edu.mayo.cts2.framework.model.core.ValueSetDefinitionReference;
import edu.mayo.cts2.framework.model.core.ValueSetReference;
import edu.mayo.cts2.framework.model.core.types.AssociationDirection;
import edu.mayo.cts2.framework.model.core.types.CompleteDirectory;
import edu.mayo.cts2.framework.model.directory.DirectoryResult;
import edu.mayo.cts2.framework.model.entity.EntityDirectoryEntry;
import edu.mayo.cts2.framework.model.extension.LocalIdValueSetDefinition;
import edu.mayo.cts2.framework.model.service.core.EntityNameOrURI;
import edu.mayo.cts2.framework.model.service.core.NameOrURI;
import edu.mayo.cts2.framework.model.valuesetdefinition.AssociatedEntitiesReference;
import edu.mayo.cts2.framework.model.valuesetdefinition.CompleteCodeSystemReference;
import edu.mayo.cts2.framework.model.valuesetdefinition.CompleteValueSetReference;
import edu.mayo.cts2.framework.model.valuesetdefinition.ExternalValueSetDefinition;
import edu.mayo.cts2.framework.model.valuesetdefinition.PropertyQueryReference;
import edu.mayo.cts2.framework.model.valuesetdefinition.ResolvedValueSet;
import edu.mayo.cts2.framework.model.valuesetdefinition.ResolvedValueSetHeader;
import edu.mayo.cts2.framework.model.valuesetdefinition.SpecificEntityList;
import edu.mayo.cts2.framework.model.valuesetdefinition.ValueSetDefinition;
import edu.mayo.cts2.framework.model.valuesetdefinition.ValueSetDefinitionEntry;
import edu.mayo.cts2.framework.model.valuesetdefinition.types.LeafOrAll;
import edu.mayo.cts2.framework.model.valuesetdefinition.types.TransitiveClosure;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.EntityReferenceAndHref;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.EntityReferenceResolver;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.ServiceLookup;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.SetUtilities;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.Utilities;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.ValueSetDefinitionSharedServiceBase;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.queries.AssociationQueryBuilder;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.valueSetDefinitionResolution.utility.CustomURIAndEntityName;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.valueSetDefinitionResolution.utility.ResolveReturn;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.valueSetDefinitionResolution.utility.ResultCache;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.valueSetDefinitionResolution.utility.ValueSetDefinitionEntryComparator;
import edu.mayo.cts2.framework.service.profile.association.AssociationQuery;
import edu.mayo.cts2.framework.service.profile.association.AssociationQueryService;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ResolvedValueSetResolutionEntityQuery;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ResolvedValueSetResult;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ValueSetDefinitionResolutionService;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.name.ValueSetDefinitionReadId;

@Component("valueSetDefinitionResolutionServiceImpl")
public class ValueSetDefinitionResolutionServiceImpl extends ValueSetDefinitionSharedServiceBase implements ValueSetDefinitionResolutionService
{
	// TODO TEST cycle detection
	// TODO TEST predicate lookup on 'resolveEntity' and see if this actually works for predicates, or if I need to customize it.
	// TODO TEST threading
	// TODO TEST property query reference
	// TODO QUESTION Kevin about all of these boilerplate things above that I'm skipping
	// TODO read timeouts on Cts2RestClient code

	/*
	 * This is used to cache entire result objects, so that we can instantly answer a request for page 2 of a query,
	 * as long as they didn't change any aspects of the query. Results time out automatically after 5 minutes.
	 */
	private Cache<Long, ResultCache> resultCache_ = CacheBuilder.newBuilder().concurrencyLevel(2).maximumSize(10).expireAfterWrite(5, TimeUnit.MINUTES).build();

	/*
	 * A thread pool used in the resolution of various entities - as those operations are usually handled by other services that
	 * may be a slow network hop away.
	 */
	private ExecutorService threadPool_ = new ThreadPoolExecutor(2, 15, 5, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(), new ThreadFactory()
	{
		@Override
		public Thread newThread(Runnable r)
		{
			Thread thread = new Thread(r);
			thread.setDaemon(true);
			thread.setName("ValueSetDefinition-ResolveThread");
			return thread;
		}
	});

	@Override
	public Set<PredicateReference> getKnownProperties()
	{
		return new HashSet<PredicateReference>();  // not sure what this is for - don't see any examples
	}

	@Override
	public Set<? extends MatchAlgorithmReference> getSupportedMatchAlgorithms()
	{
		// Arrays.asList(new MatchAlgorithmReference[] { StandardMatchAlgorithmReference.EXACT_MATCH.getMatchAlgorithmReference()})
		return new HashSet<MatchAlgorithmReference>();
	}

	@Override
	public Set<? extends ComponentReference> getSupportedSearchReferences()
	{
		return new HashSet<ComponentReference>();  // Not yet supporting any of these - I think this used for the filtering on the final results?
	}

	@Override
	public Set<? extends ComponentReference> getSupportedSortReferences()
	{
		return new HashSet<ComponentReference>();  // not supporting any sorts at the moment
	}

	/**
	 * Resolve definition as complete set.
	 * 
	 * @param definitionId the definition id
	 * @param codeSystemVersions the code system versions to execute against
	 * @param tag the tag (if any) of the code system versions to use
	 * @param readContext the read context
	 * @return the resolved value set
	 */
	@Override
	public ResolvedValueSet resolveDefinitionAsCompleteSet(ValueSetDefinitionReadId definitionId, Set<NameOrURI> codeSystemVersions, NameOrURI tag,
			ResolvedReadContext readContext)
	{
		logger_.debug("resolveDefinitionAsCompleteSet {}, {}, {}, {}", definitionId, codeSystemVersions, tag, readContext);
		ResolvedValueSet resolvedValueSet = new ResolvedValueSet();
		ResolveReturn rr = resolveHelper(definitionId, codeSystemVersions, tag, null, null, readContext, null, true);
		resolvedValueSet.setEntry(rr.getItems());
		resolvedValueSet.setResolutionInfo(rr.getHeader());
		return resolvedValueSet;
	}

	/**
	 * Resolve a {@link ValueSetDefinition} into a set of {@link EntitySynopsis} entries.
	 * 
	 * @param definitionId the definition id
	 * @param codeSystemVersions the code system versions to execute against
	 * @param tag the tag (if any) of the code system versions to use
	 * @param query the query to filter the returned results
	 * @param readContext the read context
	 * @param page the page
	 * @return the resolved value set result
	 */
	@Override
	public ResolvedValueSetResult<URIAndEntityName> resolveDefinition(ValueSetDefinitionReadId definitionId, Set<NameOrURI> codeSystemVersions, NameOrURI tag,
			ResolvedValueSetResolutionEntityQuery query, SortCriteria sortCriteria, ResolvedReadContext readContext, Page page)
	{
		logger_.debug("resolveDefinition {}, {}, {}, {}, {}, {}, {}", definitionId, codeSystemVersions, tag, query, sortCriteria, readContext, page);

		ResolveReturn rr = resolveHelper(definitionId, codeSystemVersions, tag, query, sortCriteria, readContext, page, true);
		return new ResolvedValueSetResult<URIAndEntityName>(rr.getHeader(), rr.getItems(), rr.isAtEnd());
	}

	/**
	 * Resolve a {@link ValueSetDefinition} into a set of {@link EntityDirectoryEntry} entries.
	 * 
	 * @param definitionId the definition id
	 * @param codeSystemVersions the code system versions to execute against
	 * @param tag the tag (if any) of the code system versions to use
	 * @param query the query to filter the returned results
	 * @param readContext the read context
	 * @param page the page
	 * @return the directory result
	 */
	@Override
	public ResolvedValueSetResult<EntityDirectoryEntry> resolveDefinitionAsEntityDirectory(ValueSetDefinitionReadId definitionId, Set<NameOrURI> codeSystemVersions,
			NameOrURI tag, ResolvedValueSetResolutionEntityQuery query, SortCriteria sortCriteria, ResolvedReadContext readContext, Page page)
	{
		logger_.debug("resolveDefinitionAsEntityDirectory {}, {}, {}, {}, {}, {}, {}", definitionId, codeSystemVersions, tag, query, sortCriteria, readContext, page);
		ResolveReturn rr = resolveHelper(definitionId, codeSystemVersions, tag, query, sortCriteria, readContext, page, false);
		ResolvedValueSetResult<EntityDirectoryEntry> rvsr = new ResolvedValueSetResult<EntityDirectoryEntry>(rr.getHeader(), rr.getEntityDirectoryEntry(), rr.isAtEnd());
		return rvsr;
	}

	private ResolveReturn resolveHelper(ValueSetDefinitionReadId definitionId, Set<NameOrURI> codeSystemVersions, NameOrURI tag,
			ResolvedValueSetResolutionEntityQuery query, SortCriteria sortCriteria, final ResolvedReadContext readContext, Page page, boolean entitySynopsis)
	{
		logger_.debug("resolveHelper {}, {}, {}, {}, {}, {}, {}, {}", definitionId, codeSystemVersions, tag, query, sortCriteria, readContext, page, entitySynopsis);

		Long cacheKey = makeCacheKey(definitionId, codeSystemVersions, tag, query, sortCriteria, readContext, entitySynopsis);
		ResultCache rc = resultCache_.getIfPresent(cacheKey);
		if (rc != null)
		{
			return new ResolveReturn(rc, page);
		}

		if (sortCriteria != null && sortCriteria.getEntryAsReference().size() > 0)
		{
			// TODO LATER - implement sorting
			throw new UnsupportedOperationException("Sorting is not yet implemented in this service");
		}

		// TODO QUESTION how to handle query
		// TODO handle tag, codeSystemVersions properly

		ArrayList<EntityReferenceResolver> result = new ArrayList<>();
		ArrayList<ResolvedValueSetHeader> includesResolvedValueSets = new ArrayList<>();
		HashSet<CodeSystemVersionReference> resolvedUsingCodeSystems = new HashSet<>();

		LocalIdValueSetDefinition localIdValueSetDefinition = utilities_.lookupValueSetDefinition(definitionId, readContext);
		ValueSetDefinition vsd = localIdValueSetDefinition.getResource();
		String valueSetDefinitionName = localIdValueSetDefinition.getLocalID();

		ArrayList<ArrayList<EntityReferenceResolver>> tempResults = new ArrayList<>();

		Collections.sort(vsd.getEntryAsReference(), new ValueSetDefinitionEntryComparator());

		for (ValueSetDefinitionEntry valueSetDefinitionEntry : vsd.getEntryAsReference())
		{
			Object entry = valueSetDefinitionEntry.getChoiceValue();
			ArrayList<EntityReferenceResolver> itemList = new ArrayList<>();

			if (entry instanceof AssociatedEntitiesReference)
			{
				AssociatedEntitiesReference aer = (AssociatedEntitiesReference) entry;
				List<EntityReferenceResolver> items = resolveAssociations(aer, readContext);
				itemList.addAll(items);
			}
			else if (entry instanceof PropertyQueryReference)
			{
				PropertyQueryReference pqr = (PropertyQueryReference) entry;
				CodeSystemVersionCatalogEntry csv = utilities_.lookupCodeSystemVersion(pqr.getCodeSystem(), pqr.getCodeSystemVersion(), readContext);

				if (pqr.getFilter() == null)
				{
					throw new IllegalArgumentException("A property filter must be provided within a PropertyQueryReference");
				}
				ResolvedFilter resolvedFilter = new ResolvedFilter();
				resolvedFilter.setMatchAlgorithmReference(pqr.getFilter().getMatchAlgorithm());
				resolvedFilter.setMatchValue(pqr.getFilter().getMatchValue());
				resolvedFilter.setComponentReference(pqr.getFilter());

				Iterator<EntityReferenceResolver> entities = utilities_.getEntities(csv.getCodeSystemVersionName(), csv.getVersionOf().getContent(),
						csv.getEntityDescriptions(), resolvedFilter, readContext);
				while (entities.hasNext())
				{
					itemList.add(entities.next());
				}
			}
			else if (entry instanceof CompleteValueSetReference)
			{
				CompleteValueSetReference completeValueSetRef = (CompleteValueSetReference) entry;
				ValueSetDefinitionReadId nestedValueSetDefinitionReadId = new ValueSetDefinitionReadId(completeValueSetRef.getValueSetDefinition().getUri());
				nestedValueSetDefinitionReadId.setName(completeValueSetRef.getValueSetDefinition().getContent());

				NameOrURI nestedValueSet = new NameOrURI();
				if (completeValueSetRef.getValueSet() != null)
				{
					nestedValueSet.setName(completeValueSetRef.getValueSet().getContent());
					nestedValueSet.setUri(completeValueSetRef.getValueSet().getUri());
				}
				nestedValueSetDefinitionReadId.setValueSet(nestedValueSet);

				HashSet<NameOrURI> nestedCodeSystemVersions = new HashSet<NameOrURI>();
				if (completeValueSetRef.getReferenceCodeSystemVersion() != null)
				{
					NameOrURI nestedCodeSystemVersion = new NameOrURI();
					nestedCodeSystemVersion.setName(completeValueSetRef.getReferenceCodeSystemVersion().getVersion().getContent());
					nestedCodeSystemVersion.setUri(completeValueSetRef.getReferenceCodeSystemVersion().getVersion().getUri());
					nestedCodeSystemVersions.add(nestedCodeSystemVersion);
				}

				ResolvedValueSet rvs = resolveDefinitionAsCompleteSet(nestedValueSetDefinitionReadId, nestedCodeSystemVersions, tag, readContext);

				for (URIAndEntityName item : rvs.getEntryAsReference())
				{
					itemList.add(new EntityReferenceResolver(item));
				}
				includesResolvedValueSets.add(rvs.getResolutionInfo());
			}
			else if (entry instanceof CompleteCodeSystemReference)
			{
				CompleteCodeSystemReference completeCodeSystemRef = (CompleteCodeSystemReference) entry;
				CodeSystemVersionCatalogEntry cs = utilities_.lookupCodeSystemVersion(completeCodeSystemRef.getCodeSystem(),
						completeCodeSystemRef.getCodeSystemVersion(), readContext);
				Iterator<EntityReferenceResolver> entities = utilities_.getEntities(cs.getCodeSystemVersionName(), cs.getVersionOf().getContent(),
						cs.getEntityDescriptions(), null, readContext);
				while (entities.hasNext())
				{
					itemList.add(entities.next());
				}
			}
			else if (entry instanceof SpecificEntityList)
			{
				SpecificEntityList sel = (SpecificEntityList) entry;

				// Resolving these entities could be slow - so kick them into the thread pool.
				ArrayList<Callable<EntityReferenceAndHref>> tasks = new ArrayList<>(sel.getReferencedEntityAsReference().size());
				{
					for (final URIAndEntityName entity : sel.getReferencedEntityAsReference())
					{
						tasks.add(new Callable<EntityReferenceAndHref>()
						{
							@Override
							public EntityReferenceAndHref call() throws Exception
							{
								EntityReferenceAndHref er = utilities_.resolveEntityReference(entity, readContext);
								if (er == null)
								{
									logger_.error("The entity '" + entity.toString() + "' from a SpecificEntityList could not be resolved");
									throw new IllegalArgumentException("The entity '" + entity.toString() + "' from a SpecificEntityList could not be resolved");
//									er = new EntityReference();
//									er.setAbout(entity.getUri());
//									ScopedEntityName name = new ScopedEntityName();
//									name.setName(entity.getName());
//									name.setNamespace(entity.getNamespace());
//									er.setName(name);
								}
								return er;
							}
						});
					}
				}

				try
				{
					// This automatically waits for everything to finish.
					List<Future<EntityReferenceAndHref>> results = threadPool_.invokeAll(tasks);
					for (Future<EntityReferenceAndHref> futureEntityReference : results)
					{
						itemList.add(new EntityReferenceResolver(futureEntityReference.get()));
					}
				}
				catch (ExecutionException e)
				{
					if (e.getCause() instanceof RuntimeException)
					{
						throw (RuntimeException) e.getCause();
					}
					throw new RuntimeException("Unexpected", e.getCause());
				}
				catch (InterruptedException e)
				{
					throw new RuntimeException("Unexpected Interrupt", e);
				}
			}
			else if (entry instanceof ExternalValueSetDefinition)
			{
				throw new UnsupportedOperationException("This service does not know how to resolve an ExternalValueSetDefinition");
			}
			tempResults.add(itemList);
		}

		for (int i = 0; i < vsd.getEntryAsReference().size(); i++)
		{
			new SetUtilities<EntityReferenceResolver>().handleSet(vsd.getEntry(i).getOperator(), result, tempResults.get(i));
		}

		resolveMissingEntityDesignations(result, readContext);
		
		for (EntityReferenceResolver ere : result)
		{
			for (DescriptionInCodeSystem description : ere.getEntityReference().getKnownEntityDescriptionAsReference())
			{
				resolvedUsingCodeSystems.add(description.getDescribingCodeSystemVersion());
			}
		}

		ResolvedValueSetHeader header = buildResolvedValueSetHeader(vsd.getDefinedValueSet().getContent(), vsd.getDefinedValueSet().getUri(), valueSetDefinitionName,
				vsd.getAbout(), includesResolvedValueSets, resolvedUsingCodeSystems);

		ResultCache resultCache = new ResultCache(result, header);

		resultCache_.put(cacheKey, resultCache);

		return new ResolveReturn(resultCache, page);
	}

	private ResolvedValueSetHeader buildResolvedValueSetHeader(String valueSetName, String valueSetURI, String valueSetDefinitionName, String valueSetDefinitionURI,
			List<ResolvedValueSetHeader> includesResolvedValueSets, Collection<CodeSystemVersionReference> resolvedUsingCodeSystems)
	{
		ResolvedValueSetHeader resolvedValueSetHeader = new ResolvedValueSetHeader();

		ValueSetDefinitionReference valueSetDefinitionReference = new ValueSetDefinitionReference();

		ValueSetReference vsr = new ValueSetReference(valueSetName);
		vsr.setHref(Utilities.getUrlConstructor().createValueSetUrl(valueSetName));
		vsr.setUri(valueSetURI);
		valueSetDefinitionReference.setValueSet(vsr);

		NameAndMeaningReference valueSetDefinition = new NameAndMeaningReference(valueSetDefinitionName);
		valueSetDefinition.setUri(valueSetDefinitionURI);
		valueSetDefinition.setHref(Utilities.getUrlConstructor().createValueSetDefinitionUrl(valueSetName, valueSetDefinitionName));
		valueSetDefinitionReference.setValueSetDefinition(valueSetDefinition);

		resolvedValueSetHeader.setResolutionOf(valueSetDefinitionReference);

		resolvedValueSetHeader.setIncludesResolvedValueSet(includesResolvedValueSets);
		resolvedValueSetHeader.getResolvedUsingCodeSystemAsReference().addAll(resolvedUsingCodeSystems);

		return resolvedValueSetHeader;
	}

	private void resolveMissingEntityDesignations(List<EntityReferenceResolver> items, final ResolvedReadContext readContext)
	{
		final CountDownLatch cdl = new CountDownLatch(items.size());

		// Throw these entity resolution jobs at the thread pool, as they take a relatively long time against remote services.
		for (final EntityReferenceResolver item : items)
		{
			threadPool_.execute(new Runnable()
			{
				@Override
				public void run()
				{
					try
					{
						item.resolveEntity(readContext);
					}
					catch (Exception e)
					{
						logger_.warn("Unexpected error resolving designation for Entity " + item, e);
					}
					finally
					{
						cdl.countDown();
					}
				}
			});
		}

		try
		{
			// Wait for all of our resolve tasks to finish.
			cdl.await();
		}
		catch (InterruptedException e)
		{
			logger_.warn("Unexpected interrupt", e);
			throw new RuntimeException("Unexpected interrupt", e);
		}
	}

	private List<EntityReferenceResolver> resolveAssociations(AssociatedEntitiesReference aer, ResolvedReadContext readContext)
	{
		// fix my mess with the various codeSystems here
		CodeSystemVersionCatalogEntry associationVersionCodeSystemInfo = utilities_.lookupCodeSystemVersion(aer.getCodeSystem(), aer.getCodeSystemVersion(), readContext);

		URIAndEntityName referencedEntity = aer.getReferencedEntity();
		if (aer.getReferencedEntity() == null)
		{
			throw new IllegalArgumentException("Referenced Entity is required");
		}
		else if (StringUtils.isBlank(referencedEntity.getName()))
		{
			EntityReference er = utilities_.resolveEntityReference(aer.getReferencedEntity(), readContext).getEntityReference();
			referencedEntity.setName(er.getName().getName());
			referencedEntity.setNamespace(er.getName().getNamespace());
		}

		try
		{
			utilities_.resolveEntityReference(aer.getPredicate(), readContext);
		}
		catch (Exception e)
		{
			// TODO QUESTION does predicate need to be resolvable?
			logger_.warn("Predicate lookup failed, will continue using the predicate as entered", e);
		}

		HashSet<CustomURIAndEntityName> resultHolder = new HashSet<CustomURIAndEntityName>();

		processLevel(aer.getReferencedEntity(), aer.getDirection(), associationVersionCodeSystemInfo, aer.getTransitivity(), aer.getPredicate().getUri(),
				LeafOrAll.LEAF_ONLY == aer.getLeafOnly(), ((aer.getCodeSystemVersion() != null && aer.getCodeSystemVersion().getVersion() != null) ? aer
						.getCodeSystemVersion().getVersion().getHref() : null), resultHolder, readContext);

		ArrayList<EntityReferenceResolver> results = new ArrayList<>(resultHolder.size());

		for (CustomURIAndEntityName entity : resultHolder)
		{
			results.add(new EntityReferenceResolver(entity.getEntity()));
		}

		return results;
	}

	private void processLevel(URIAndEntityName entity, AssociationDirection direction, CodeSystemVersionCatalogEntry associationCodeSystemVersion,
			TransitiveClosure transitivity, String predicateURI, boolean leafOnly, String altServiceHrefForCodeSystem, HashSet<CustomURIAndEntityName> resultHolder,
			ResolvedReadContext readContext)
	{
		AssociationQueryService aqs = ServiceLookup.getLocalAssociationQueryService();
		ArrayList<CustomURIAndEntityName> thisLevelResults = new ArrayList<>();

		// CodeSystemVersionReference entityCodeSystemVersion = getCodeSystemVersionForEntity(entity, readContext);

		if (aqs != null)
		{
			Page page = new Page();
			page.setMaxToReturn(500);
			page.setPage(0);

			AssociationQuery query = AssociationQueryBuilder.build(readContext);

			NameOrURI codeSystemVersion = new NameOrURI();
			codeSystemVersion.setUri(associationCodeSystemVersion.getAbout());
			query.getRestrictions().setCodeSystemVersion(codeSystemVersion);

			EntityNameOrURI predicate = new EntityNameOrURI();
			predicate.setUri(predicateURI);
			query.getRestrictions().setPredicate(predicate);

			EntityNameOrURI referencedEntity = new EntityNameOrURI();
			referencedEntity.setUri(entity.getUri());
			ScopedEntityName sne = new ScopedEntityName();
			sne.setName(entity.getName());
			sne.setNamespace(entity.getNamespace());
			referencedEntity.setEntityName(sne);

			if (AssociationDirection.SOURCE_TO_TARGET == direction)
			{
				query.getRestrictions().setSourceEntity(referencedEntity);
			}
			else if (AssociationDirection.TARGET_TO_SOURCE == direction)
			{
				query.getRestrictions().setTargetEntity(referencedEntity);
			}
			else
			{
				throw new IllegalArgumentException("Unexpected direction parameter - " + direction);
			}
			while (true)
			{
				DirectoryResult<AssociationDirectoryEntry> temp = aqs.getResourceSummaries(query, null, page);
				for (AssociationDirectoryEntry ade : temp.getEntries())
				{
					if (ade.getAssertedBy().getCodeSystem().getUri().equals(associationCodeSystemVersion.getVersionOf().getUri()))
					// TODO fix this
					// &&
					// (StringUtils.isBlank(associationCodeSystemVersionURI) ||
					// associationCodeSystemVersionURI.equals(ade.getAssertedBy().getVersion().getUri())))
					{
						thisLevelResults.add(new CustomURIAndEntityName(ade.getTarget().getEntity()));
					}
				}
				if (temp.isAtEnd())
				{
					break;
				}
				else
				{
					page.setPage(page.getPage() + 1);
				}
			}
		}
		else
		{
			boolean sourceToTarget;
			if (AssociationDirection.SOURCE_TO_TARGET == direction)
			{
				sourceToTarget = true;
			}
			else if (AssociationDirection.TARGET_TO_SOURCE == direction)
			{
				sourceToTarget = false;
			}
			else
			{
				throw new IllegalArgumentException("Unexpected direction parameter - " + direction);
			}

			String href = utilities_.makeAssociationURL(associationCodeSystemVersion.getCodeSystemVersionName(),
					associationCodeSystemVersion.getVersionOf().getContent(), entity.getName(), sourceToTarget, altServiceHrefForCodeSystem, readContext);
			if (href == null)
			{
				throw new RuntimeException("No service is available to resolve the hierarchy");
			}

			// TODO BUG switch over to predicate filtering directly, when fixed - https://github.com/cts2/cts2-framework/issues/28
			thisLevelResults.addAll(gatherAssociationDirectoryResults(href, predicateURI, associationCodeSystemVersion));
		}

		if (TransitiveClosure.DIRECTLY_ASSOCIATED == transitivity)
		{
			// noop - only want one level.
		}
		else if (TransitiveClosure.TRANSITIVE_CLOSURE == transitivity)
		{
			Iterator<CustomURIAndEntityName> it = thisLevelResults.iterator();
			while (it.hasNext())
			{
				CustomURIAndEntityName nextItem = it.next();

				if (resultHolder.contains(nextItem))
				{
					// Just detected a cycle. No further processing for this item.
					it.remove();
					continue;
				}

				int sizeBefore = resultHolder.size();
				processLevel(nextItem.getEntity(), direction, associationCodeSystemVersion, transitivity, predicateURI, leafOnly, altServiceHrefForCodeSystem,
						resultHolder, readContext);
				if (leafOnly && resultHolder.size() > sizeBefore)
				{
					// There were children from this node
					it.remove();
				}
			}
		}
		else
		{
			throw new IllegalArgumentException("Invalid Transitivity selection");
		}
		for (CustomURIAndEntityName item : thisLevelResults)
		{
			if (!resultHolder.add(item))
			{
				throw new RuntimeException("Design error in cycle detection!");
			}
		}
	}

	private ArrayList<CustomURIAndEntityName> gatherAssociationDirectoryResults(String href, String predicateURI,
			CodeSystemVersionCatalogEntry associationCodeSystemVersion)
	{
		AssociationDirectory result = Cts2RestClient.instance().getCts2Resource(href, AssociationDirectory.class);
		ArrayList<CustomURIAndEntityName> resultHolder = new ArrayList<>();

		for (AssociationDirectoryEntry ade : result.getEntryAsReference())
		{
			if (ade.getPredicate().getUri().equals(predicateURI)
					&& ade.getAssertedBy().getCodeSystem().getUri().equals(associationCodeSystemVersion.getVersionOf().getUri()))
			// TODO fix this
			// &&
			// (StringUtils.isBlank(associationCodeSystemVersionURI) ||
			// associationCodeSystemVersionURI.equals(ade.getAssertedBy().getVersion().getUri())))
			{
				resultHolder.add(new CustomURIAndEntityName(ade.getTarget().getEntity()));
			}
		}
		if (result.getComplete() == CompleteDirectory.PARTIAL && StringUtils.isNotBlank(result.getNext()))
		{
			resultHolder.addAll(gatherAssociationDirectoryResults(result.getNext(), predicateURI, associationCodeSystemVersion));
		}
		return resultHolder;
	}

	private Long makeCacheKey(ValueSetDefinitionReadId definitionId, Set<NameOrURI> codeSystemVersions, NameOrURI tag, ResolvedValueSetResolutionEntityQuery query,
			SortCriteria sortCriteria, ResolvedReadContext readContext, boolean entitySynopsis)
	{
		long result = 1;
		result = 37 * result + (definitionId == null ? 0 : definitionId.hashCode());
		result = 37 * result + (codeSystemVersions == null ? 0 : codeSystemVersions.hashCode());
		result = 37 * result + (tag == null ? 0 : tag.hashCode());
		result = 37 * result + hashQuery(query);
		result = 37 * result + (sortCriteria == null ? 0 : sortCriteria.hashCode());
		result = 37 * result + (readContext == null ? 0 : readContext.hashCode());
		result = 37 * result + new Boolean(entitySynopsis).hashCode();
		return result;
	}

	private long hashQuery(ResolvedValueSetResolutionEntityQuery query)
	{
		long result = 1;
		if (query != null)
		{
			result = 37 * result + ((query.getQuery() == null) ? 0 : query.getQuery().hashCode());
			result = 37 * result
					+ ((query.getResolvedValueSetResolutionEntityRestrictions() == null) ? 0 : query.getResolvedValueSetResolutionEntityRestrictions().hashCode());
			result = 37 * result + ((query.getFilterComponent() == null) ? 0 : query.getFilterComponent().hashCode());
		}
		return result;
	}
}