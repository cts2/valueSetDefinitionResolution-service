package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.valueSetDefinitionResolution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
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
import edu.mayo.cts2.framework.core.timeout.Timeout;
import edu.mayo.cts2.framework.model.association.AssociationDirectory;
import edu.mayo.cts2.framework.model.association.AssociationDirectoryEntry;
import edu.mayo.cts2.framework.model.codesystemversion.CodeSystemVersionCatalogEntry;
import edu.mayo.cts2.framework.model.codesystemversion.CodeSystemVersionCatalogEntrySummary;
import edu.mayo.cts2.framework.model.command.Page;
import edu.mayo.cts2.framework.model.command.ResolvedFilter;
import edu.mayo.cts2.framework.model.command.ResolvedReadContext;
import edu.mayo.cts2.framework.model.core.CodeSystemReference;
import edu.mayo.cts2.framework.model.core.CodeSystemVersionReference;
import edu.mayo.cts2.framework.model.core.ComponentReference;
import edu.mayo.cts2.framework.model.core.DescriptionInCodeSystem;
import edu.mayo.cts2.framework.model.core.EntityReference;
import edu.mayo.cts2.framework.model.core.MatchAlgorithmReference;
import edu.mayo.cts2.framework.model.core.NameAndMeaningReference;
import edu.mayo.cts2.framework.model.core.PredicateReference;
import edu.mayo.cts2.framework.model.core.ScopedEntityName;
import edu.mayo.cts2.framework.model.core.SortCriteria;
import edu.mayo.cts2.framework.model.core.SortCriterion;
import edu.mayo.cts2.framework.model.core.URIAndEntityName;
import edu.mayo.cts2.framework.model.core.ValueSetDefinitionReference;
import edu.mayo.cts2.framework.model.core.ValueSetReference;
import edu.mayo.cts2.framework.model.core.types.AssociationDirection;
import edu.mayo.cts2.framework.model.core.types.CompleteDirectory;
import edu.mayo.cts2.framework.model.directory.DirectoryResult;
import edu.mayo.cts2.framework.model.entity.EntityDirectoryEntry;
import edu.mayo.cts2.framework.model.exception.UnspecifiedCts2Exception;
import edu.mayo.cts2.framework.model.extension.LocalIdValueSetDefinition;
import edu.mayo.cts2.framework.model.service.core.EntityNameOrURI;
import edu.mayo.cts2.framework.model.service.core.NameOrURI;
import edu.mayo.cts2.framework.model.service.exception.UnknownCodeSystemVersion;
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
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.CodeSystemVersionCatalogEntryAndHref;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.EntityReferenceAndHref;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.EntityReferenceResolver;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.EntityReferenceResolverComparator;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.ExceptionBuilder;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.SetUtilities;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.SupportedSorts;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.ValueSetDefinitionSharedServiceBase;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.queries.AssociationQueryBuilder;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.valueSetDefinitionResolution.utility.CustomURIAndEntityName;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.valueSetDefinitionResolution.utility.ResolveReturn;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.valueSetDefinitionResolution.utility.ResultCache;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.valueSetDefinitionResolution.utility.SortCriterionComparator;
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
	// TODO TEST threading
	// TODO TEST property query reference
	// TODO QUESTION Kevin about all of these boilerplate things above that I'm skipping

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
		HashSet<ComponentReference> sorts = new HashSet<ComponentReference>();  // not supporting any sorts at the moment
		sorts.add(SupportedSorts.ALPHA_NUMERIC.asComponentReference());
		sorts.add(SupportedSorts.ALPHABETIC.asComponentReference());
		return sorts;
		
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
		ResolveReturn rr = resolveHelper(definitionId, codeSystemVersions, tag, null, null, readContext, null);
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

		ResolveReturn rr = resolveHelper(definitionId, codeSystemVersions, tag, query, sortCriteria, readContext, page);
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
		ResolveReturn rr = resolveHelper(definitionId, codeSystemVersions, tag, query, sortCriteria, readContext, page);
		ResolvedValueSetResult<EntityDirectoryEntry> rvsr = new ResolvedValueSetResult<EntityDirectoryEntry>(rr.getHeader(), rr.getEntityDirectoryEntry(), rr.isAtEnd());
		return rvsr;
	}

	private ResolveReturn resolveHelper(ValueSetDefinitionReadId definitionId, Set<NameOrURI> codeSystemVersions, final NameOrURI tag,
			ResolvedValueSetResolutionEntityQuery query, SortCriteria sortCriteria, final ResolvedReadContext readContext, Page page)
	{
		logger_.debug("resolveHelper {}, {}, {}, {}, {}, {}, {}", definitionId, codeSystemVersions, tag, query, sortCriteria, readContext, page);

		Long cacheKey = makeCacheKey(definitionId, codeSystemVersions, tag, query, sortCriteria, readContext);
		ResultCache rc = resultCache_.getIfPresent(cacheKey);
		if (rc != null)
		{
			return new ResolveReturn(rc, page);
		}
		
		ArrayList<SupportedSorts> resolvedSortCriteria = new ArrayList<>();

		if (sortCriteria != null)
		{
			Collections.sort(sortCriteria.getEntryAsReference(), new SortCriterionComparator());
			//verify the sort criteria is valid
			for (SortCriterion sc : sortCriteria.getEntryAsReference())
			{
				boolean found = false;
				for (SupportedSorts ss : SupportedSorts.values())
				{
					if (ss.getNiceName().equals(sc.getSortElement().getSpecialReference()))
					{
						resolvedSortCriteria.add(ss);
						found = true;
						break;
					}
				}
				if (!found)
				{
					throw new UnspecifiedCts2Exception("Unsupported sort algorithm '" + sc.getSortElement().getSpecialReference() + "'");
				}
			}
		}

		// TODO QUESTION how to handle query
		
		//Lookup all of the codeSystemVersions
		final HashMap<String, CodeSystemVersionCatalogEntryAndHref> resolvedCodeSystemVersions = new HashMap<>();
		if (codeSystemVersions != null)
		{
			for (NameOrURI nou : codeSystemVersions)
			{
				try
				{
					CodeSystemVersionCatalogEntryAndHref csvce = utilities_.lookupCodeSystemVersion(nou, null, readContext);
					resolvedCodeSystemVersions.put(csvce.getCodeSystemVersionCatalogEntry().getAbout(), csvce);
				}
				catch (Exception e)
				{
					throw ExceptionBuilder.buildUnknownCodeSystemVersion("The requested code system version '" + nou.getName() + ":" + nou.getUri() +
							"' could not be resolved");
				}
			}
		}

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
			if (Timeout.isTimeLimitExceeded())
			{
				throw new RuntimeException("Notified of timeout");
			}
			Object entry = valueSetDefinitionEntry.getChoiceValue();
			ArrayList<EntityReferenceResolver> itemList = new ArrayList<>();

			if (entry instanceof AssociatedEntitiesReference)
			{
				AssociatedEntitiesReference aer = (AssociatedEntitiesReference) entry;
				List<EntityReferenceResolver> items = resolveAssociations(aer, resolvedCodeSystemVersions, tag, readContext);
				itemList.addAll(items);
			}
			else if (entry instanceof PropertyQueryReference)
			{
				PropertyQueryReference pqr = (PropertyQueryReference) entry;
				
				CodeSystemVersionCatalogEntry csv = pickBestCodeSystemVersion(pqr.getCodeSystemVersion(), pqr.getCodeSystem(), tag, resolvedCodeSystemVersions, 
						readContext).getCodeSystemVersionCatalogEntry();

				if (pqr.getFilter() == null)
				{
					throw new UnspecifiedCts2Exception("A property filter must be provided within a PropertyQueryReference");
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

				Page subPage = new Page();
				
				ResolvedValueSetHeader rvsh = null;
				while (true)
				{
					//TODO QUESTION about how nestedCodeSystemVersions are handled with the specified codesystemVersions
					ResolveReturn rr = resolveHelper(nestedValueSetDefinitionReadId, nestedCodeSystemVersions, tag, null, null, readContext, subPage);
					if (rvsh == null)
					{
						rvsh = rr.getHeader();
					}
					for (EntityDirectoryEntry item : rr.getEntityDirectoryEntry())
					{
						itemList.add(new EntityReferenceResolver(item));
					}
					if (rr.isAtEnd())
					{
						break;
					}
					else
					{
						subPage.setPage(subPage.getEnd() + 1);
					}
				}				
				includesResolvedValueSets.add(rvsh);
			}
			else if (entry instanceof CompleteCodeSystemReference)
			{
				CompleteCodeSystemReference completeCodeSystemRef = (CompleteCodeSystemReference) entry;
				CodeSystemVersionCatalogEntry csv = pickBestCodeSystemVersion(completeCodeSystemRef.getCodeSystemVersion(), completeCodeSystemRef.getCodeSystem(), 
						tag, resolvedCodeSystemVersions, readContext).getCodeSystemVersionCatalogEntry();
				Iterator<EntityReferenceResolver> entities = utilities_.getEntities(csv.getCodeSystemVersionName(), csv.getVersionOf().getContent(),
						csv.getEntityDescriptions(), null, readContext);
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
								EntityReferenceAndHref er = utilities_.resolveEntityReference(entity, resolvedCodeSystemVersions, tag, readContext);
								if (er == null)
								{
									logger_.error("The entity '" + entity.toString() + "' from a SpecificEntityList could not be resolved");
									throw ExceptionBuilder.buildUnknownEntity("The entity '" + entity.toString() + "' from a SpecificEntityList could not be resolved");
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
					if (Timeout.isTimeLimitExceeded())
					{
						throw new RuntimeException("Notified of timeout");
					}
					throw new RuntimeException("Unexpected Interrupt", e);
				}
			}
			else if (entry instanceof ExternalValueSetDefinition)
			{
				throw new UnspecifiedCts2Exception("This service does not know how to resolve an ExternalValueSetDefinition");
			}
			tempResults.add(itemList);
		}

		for (int i = 0; i < vsd.getEntryAsReference().size(); i++)
		{
			new SetUtilities<EntityReferenceResolver>().handleSet(vsd.getEntry(i).getOperator(), result, tempResults.get(i));
		}
		
		if (Timeout.isTimeLimitExceeded())
		{
			throw new RuntimeException("Notified of timeout");
		}

		if (!resolveMissingEntityDesignations(result, readContext))
		{
			throw ExceptionBuilder.buildUnknownEntity("Failed to resolve all Entities");
		}
		
		for (EntityReferenceResolver ere : result)
		{
			for (DescriptionInCodeSystem description : ere.getEntityReference().getKnownEntityDescriptionAsReference())
			{
				resolvedUsingCodeSystems.add(description.getDescribingCodeSystemVersion());
			}
		}

		ResolvedValueSetHeader header = buildResolvedValueSetHeader(vsd.getDefinedValueSet().getContent(), vsd.getDefinedValueSet().getUri(), valueSetDefinitionName,
				vsd.getAbout(), includesResolvedValueSets, resolvedUsingCodeSystems);
		
		if (resolvedSortCriteria.size() > 0)
		{
			Collections.sort(result, new EntityReferenceResolverComparator(resolvedSortCriteria));
		}

		ResultCache resultCache = new ResultCache(result, header);

		resultCache_.put(cacheKey, resultCache);

		return new ResolveReturn(resultCache, page);
	}
	
	private CodeSystemVersionCatalogEntryAndHref pickBestCodeSystemVersion(CodeSystemVersionReference codeSystemVersion, CodeSystemReference codeSystem, 
			NameOrURI tag, HashMap<String, CodeSystemVersionCatalogEntryAndHref> resolvedCodeSystemVersions, ResolvedReadContext readContext)
	{
		CodeSystemVersionCatalogEntryAndHref csv = null;
		
		if (codeSystemVersion != null && codeSystemVersion.getVersion() != null)
		{
			//We use the code system specified in the ValueSet, if one is provided
			NameOrURI nameOrURI = new NameOrURI();
			nameOrURI.setName(codeSystemVersion.getVersion().getContent());
			nameOrURI.setUri(codeSystemVersion.getVersion().getUri());
			csv = utilities_.lookupCodeSystemVersion(nameOrURI, codeSystemVersion.getVersion().getHref(), readContext);
		}
		else
		{
			//If the requested code system matches one of the passed in CodeSystemVersion references, use that.
			csv = resolvedCodeSystemVersions.get(codeSystem.getUri());
			
			if (csv == null)
			{
				//still null - no match, pick a CodeSystemVersion that matches the requested tag, or failing that - CURRENT.
				//TODO QUESTION allow fallback to arbitrary?
				CodeSystemVersionCatalogEntrySummary csvces = utilities_.lookupCodeSystemVersionByTag(codeSystem, tag, "CURRENT", true, readContext);
				if (csvces != null)
				{
					NameOrURI nameOrURI = new NameOrURI();
					nameOrURI.setUri(csvces.getAbout());
					csv = utilities_.lookupCodeSystemVersion(nameOrURI, csvces.getHref(), readContext);
				}
			}
		}
		
		
		if (csv == null)
		{
			throw ExceptionBuilder.buildUnknownCodeSystemVersion("Could not resolved the specified or requested CodeSystemVersion for " + codeSystem);
		}
		return csv;
	}

	private ResolvedValueSetHeader buildResolvedValueSetHeader(String valueSetName, String valueSetURI, String valueSetDefinitionName, String valueSetDefinitionURI,
			List<ResolvedValueSetHeader> includesResolvedValueSets, Collection<CodeSystemVersionReference> resolvedUsingCodeSystems)
	{
		ResolvedValueSetHeader resolvedValueSetHeader = new ResolvedValueSetHeader();

		ValueSetDefinitionReference valueSetDefinitionReference = new ValueSetDefinitionReference();

		ValueSetReference vsr = new ValueSetReference(valueSetName);
		vsr.setHref(utilities_.getUrlConstructor().createValueSetUrl(valueSetName));
		vsr.setUri(valueSetURI);
		valueSetDefinitionReference.setValueSet(vsr);

		NameAndMeaningReference valueSetDefinition = new NameAndMeaningReference(valueSetDefinitionName);
		valueSetDefinition.setUri(valueSetDefinitionURI);
		valueSetDefinition.setHref(utilities_.getUrlConstructor().createValueSetDefinitionUrl(valueSetName, valueSetDefinitionName));
		valueSetDefinitionReference.setValueSetDefinition(valueSetDefinition);

		resolvedValueSetHeader.setResolutionOf(valueSetDefinitionReference);

		resolvedValueSetHeader.setIncludesResolvedValueSet(includesResolvedValueSets);
		resolvedValueSetHeader.getResolvedUsingCodeSystemAsReference().addAll(resolvedUsingCodeSystems);

		return resolvedValueSetHeader;
	}

	/**
	 * Returns true if all were resolved without error, false otherwise.
	 * @return
	 */
	private boolean resolveMissingEntityDesignations(List<EntityReferenceResolver> items, final ResolvedReadContext readContext)
	{
		ArrayList<Callable<Boolean>> tasks = new ArrayList<>();
		
		// Throw these entity resolution jobs at the thread pool, as they take a relatively long time against remote services.
		for (final EntityReferenceResolver item : items)
		{
			tasks.add(new Callable<Boolean>()
			{
				@Override
				public Boolean call() throws Exception
				{
					try
					{
						item.resolveEntity(readContext, utilities_);
						return true;
					}
					catch (Exception e)
					{
						logger_.warn("Unexpected error resolving designation for Entity " + item, e);
						return false;
					}
				}
			});
		}
		try
		{
			// Wait for all of our resolve tasks to finish.
			List<Future<Boolean>> results = threadPool_.invokeAll(tasks);
			try
			{
				for (Future<Boolean> result : results)
				{
					if (!result.get())
					{
						return false;
					}
				}
			}
			catch (ExecutionException e)
			{
				logger_.error("Unexpected error", e);
				return false;
			}
			return true;
		}
		catch (InterruptedException e)
		{
			if (Timeout.isTimeLimitExceeded())
			{
				throw new RuntimeException("Notified of timeout");
			}
			logger_.warn("Unexpected interrupt", e);
			throw new RuntimeException("Unexpected interrupt", e);
		}
	}

	private List<EntityReferenceResolver> resolveAssociations(AssociatedEntitiesReference aer, HashMap<String, CodeSystemVersionCatalogEntryAndHref> resolvedCodeSystemVersions, 
			NameOrURI tag, ResolvedReadContext readContext)
	{
		CodeSystemVersionCatalogEntryAndHref associationVersionCodeSystemInfo = pickBestCodeSystemVersion(aer.getCodeSystemVersion(), aer.getCodeSystem(), tag, 
				resolvedCodeSystemVersions, readContext);

		URIAndEntityName referencedEntity = aer.getReferencedEntity();
		if (aer.getReferencedEntity() == null)
		{
			throw new UnspecifiedCts2Exception("Referenced Entity is required");
		}
		else if (StringUtils.isBlank(referencedEntity.getName()))
		{
			//This code should never actually run... but leave it in, since its here.
			EntityReference er = utilities_.resolveEntityReference(aer.getReferencedEntity(), readContext).getEntityReference();
			referencedEntity.setName(er.getName().getName());
			referencedEntity.setNamespace(er.getName().getNamespace());
		}

		//Kevin said we didn't need to bother resolving the predicate

		HashSet<CustomURIAndEntityName> resultHolder = new HashSet<CustomURIAndEntityName>();

		processLevel(aer.getReferencedEntity(), aer.getDirection(), associationVersionCodeSystemInfo, aer.getTransitivity(), aer.getPredicate().getUri(),
				LeafOrAll.LEAF_ONLY == aer.getLeafOnly(), resultHolder, readContext);

		ArrayList<EntityReferenceResolver> results = new ArrayList<>(resultHolder.size());
		

		for (CustomURIAndEntityName entity : resultHolder)
		{
			results.add(new EntityReferenceResolver(entity.getEntity(), associationVersionCodeSystemInfo.getAsCodeSystemVersionReference()));
		}

		return results;
	}

	private void processLevel(URIAndEntityName entity, AssociationDirection direction, CodeSystemVersionCatalogEntryAndHref associationCodeSystemVersion,
			TransitiveClosure transitivity, String predicateURI, boolean leafOnly, HashSet<CustomURIAndEntityName> resultHolder, ResolvedReadContext readContext)
	{
		AssociationQueryService aqs = utilities_.getLocalAssociationQueryService();
		ArrayList<CustomURIAndEntityName> thisLevelResults = new ArrayList<>();

		// CodeSystemVersionReference entityCodeSystemVersion = getCodeSystemVersionForEntity(entity, readContext);

		boolean localServiceFail = false;
		
		if (aqs != null)
		{
			try
			{
				Page page = new Page();
				page.setMaxToReturn(500);
				page.setPage(0);
	
				AssociationQuery query = AssociationQueryBuilder.build(readContext);
	
				NameOrURI codeSystemVersion = new NameOrURI();
				codeSystemVersion.setUri(associationCodeSystemVersion.getCodeSystemVersionCatalogEntry().getAbout());
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
					throw new UnspecifiedCts2Exception("Unexpected direction parameter - " + direction);
				}
				while (true)
				{
					DirectoryResult<AssociationDirectoryEntry> temp = aqs.getResourceSummaries(query, null, page);
					for (AssociationDirectoryEntry ade : temp.getEntries())
					{
						if (associationCodeSystemVersion.getCodeSystemVersionCatalogEntry().getAbout().equals(ade.getAssertedBy().getVersion().getUri()))
						{
							thisLevelResults.add(new CustomURIAndEntityName(ade.getTarget().getEntity()));
						}
						else
						{
							logger_.warn("We crossed code systems during an association query - ignoring a result - !" + ade);
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
			catch (UnknownCodeSystemVersion e)
			{
				//TODO TEST if this is the right exception to catch 
				localServiceFail = true;
			}
		}
		
		//Try the remote lookup route, if the local didn't work
		if (localServiceFail || aqs == null)
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
				throw new UnspecifiedCts2Exception("Unexpected direction parameter - " + direction);
			}

			String href = utilities_.makeAssociationURL(associationCodeSystemVersion.getCodeSystemVersionCatalogEntry().getCodeSystemVersionName(),
					associationCodeSystemVersion.getCodeSystemVersionCatalogEntry().getVersionOf().getContent(), entity.getName(), sourceToTarget, 
					associationCodeSystemVersion.getHref(), readContext);

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
				processLevel(nextItem.getEntity(), direction, associationCodeSystemVersion, transitivity, predicateURI, leafOnly, resultHolder, readContext);
				if (leafOnly && resultHolder.size() > sizeBefore)
				{
					// There were children from this node
					it.remove();
				}
			}
		}
		else
		{
			throw new UnspecifiedCts2Exception("Invalid Transitivity selection");
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
			CodeSystemVersionCatalogEntryAndHref associationCodeSystemVersion)
	{
		AssociationDirectory result = Cts2RestClient.instance().getCts2Resource(href, AssociationDirectory.class);
		ArrayList<CustomURIAndEntityName> resultHolder = new ArrayList<>();

		for (AssociationDirectoryEntry ade : result.getEntryAsReference())
		{
			if (associationCodeSystemVersion.getCodeSystemVersionCatalogEntry().getAbout().equals(ade.getAssertedBy().getVersion().getUri()))
			{
				if (ade.getPredicate().getUri().equals(predicateURI))
				{
					resultHolder.add(new CustomURIAndEntityName(ade.getTarget().getEntity()));
				}
			}
			else
			{
				logger_.warn("We crossed code systems during an association query - ignoring a result - !" + ade);
			}
		}
		if (result.getComplete() == CompleteDirectory.PARTIAL && StringUtils.isNotBlank(result.getNext()))
		{
			resultHolder.addAll(gatherAssociationDirectoryResults(result.getNext(), predicateURI, associationCodeSystemVersion));
		}
		return resultHolder;
	}

	private Long makeCacheKey(ValueSetDefinitionReadId definitionId, Set<NameOrURI> codeSystemVersions, NameOrURI tag, ResolvedValueSetResolutionEntityQuery query,
			SortCriteria sortCriteria, ResolvedReadContext readContext)
	{
		long result = 1;
		result = 37 * result + (definitionId == null ? 0 : definitionId.hashCode());
		result = 37 * result + (codeSystemVersions == null ? 0 : codeSystemVersions.hashCode());
		result = 37 * result + (tag == null ? 0 : tag.hashCode());
		result = 37 * result + hashQuery(query);
		result = 37 * result + (sortCriteria == null ? 0 : sortCriteria.hashCode());
		result = 37 * result + (readContext == null ? 0 : readContext.hashCode());
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