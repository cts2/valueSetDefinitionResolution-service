package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.valueSetRelatedPartialServices.valueSetTransientImpl;

import java.util.ArrayList;
import java.util.Set;
import org.springframework.stereotype.Component;
import edu.mayo.cts2.framework.model.command.Page;
import edu.mayo.cts2.framework.model.core.ComponentReference;
import edu.mayo.cts2.framework.model.core.MatchAlgorithmReference;
import edu.mayo.cts2.framework.model.core.PredicateReference;
import edu.mayo.cts2.framework.model.core.SortCriteria;
import edu.mayo.cts2.framework.model.directory.DirectoryResult;
import edu.mayo.cts2.framework.model.exception.UnspecifiedCts2Exception;
import edu.mayo.cts2.framework.model.valueset.ValueSetCatalogEntry;
import edu.mayo.cts2.framework.model.valueset.ValueSetCatalogEntryListEntry;
import edu.mayo.cts2.framework.model.valueset.ValueSetCatalogEntrySummary;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ValueSetDefinitionSharedServiceBase;
import edu.mayo.cts2.framework.service.profile.valueset.ValueSetQuery;
import edu.mayo.cts2.framework.service.profile.valueset.ValueSetQueryService;

@Component("valueSetQueryServiceImpl")
public class ValueSetQueryServiceImpl extends ValueSetDefinitionSharedServiceBase implements ValueSetQueryService
{
	@Override
	public DirectoryResult<ValueSetCatalogEntrySummary> getResourceSummaries(ValueSetQuery query, SortCriteria sortCriteria, Page page)
	{
		logger_.debug("getResourceSummaries {}, {}, {}", query, sortCriteria, page);
		ArrayList<ValueSetCatalogEntrySummary> result = new ArrayList<ValueSetCatalogEntrySummary>();

		for (final ValueSetCatalogEntry vs : ValueSetStorage.getInstance().getAll())
		{
			ValueSetCatalogEntrySummary vsces = new ValueSetCatalogEntrySummary();
			vsces.setAbout(vs.getAbout());
			vsces.setFormalName(vs.getFormalName());
			vsces.setHref(utilities_.getUrlConstructor().createValueSetUrl(vs.getValueSetName()));
			vsces.setMatchStrength(1.0);
			vsces.setResourceName(vs.getValueSetName());
			vsces.setResourceSynopsis(vs.getResourceSynopsis());
			vsces.setValueSetName(vs.getValueSetName());

			// Lookup a definition, if we can find one...
			try
			{
				vsces.setCurrentDefinition(utilities_.lookupCurrentValueSetDefinitionReference(vsces.getAbout(), vsces.getHref(), vsces.getValueSetName(),
						query.getReadContext()));
			}
			catch (Exception e)
			{
				logger_.info("ValueSetDefinitionReference lookup failed", e);
			}

			result.add(vsces);
		}

		return new DirectoryResult<ValueSetCatalogEntrySummary>(result, true);
	}

	@Override
	public DirectoryResult<ValueSetCatalogEntryListEntry> getResourceList(ValueSetQuery query, SortCriteria sortCriteria, Page page)
	{
		logger_.debug("getResourceList {}, {}, {}", query, sortCriteria, page);
		ArrayList<ValueSetCatalogEntryListEntry> results = new ArrayList<ValueSetCatalogEntryListEntry>();

		for (final ValueSetCatalogEntry vs : ValueSetStorage.getInstance().getAll())
		{
			ValueSetCatalogEntryListEntry vscele = new ValueSetCatalogEntryListEntry();
			vscele.setEntry(vs);
			vscele.setHref(utilities_.getUrlConstructor().createValueSetUrl(vs.getValueSetName()));
			vscele.setMatchStrength(1.0);
			vscele.setResourceName(vs.getValueSetName());
			utilities_.updateValueSetForReturn(vs, vscele.getHref(), query.getReadContext());
			results.add(vscele);
		}
		return new DirectoryResult<ValueSetCatalogEntryListEntry>(results, true);
	}

	@Override
	public int count(ValueSetQuery query)
	{
		return ValueSetStorage.getInstance().getAll().size();
	}

	@Override
	public Set<? extends MatchAlgorithmReference> getSupportedMatchAlgorithms()
	{
		throw new UnspecifiedCts2Exception("Unsupported Operation");
	}

	@Override
	public Set<? extends ComponentReference> getSupportedSearchReferences()
	{
		throw new UnspecifiedCts2Exception("Unsupported Operation");
	}

	@Override
	public Set<? extends ComponentReference> getSupportedSortReferences()
	{
		throw new UnspecifiedCts2Exception("Unsupported Operation");
	}

	@Override
	public Set<PredicateReference> getKnownProperties()
	{
		throw new UnspecifiedCts2Exception("Unsupported Operation");
	}
}
