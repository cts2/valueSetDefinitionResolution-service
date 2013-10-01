package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.valueSetRelatedPartialServices.valueSetDefinitionTransientImpl;

import java.util.ArrayList;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;
import edu.mayo.cts2.framework.model.command.Page;
import edu.mayo.cts2.framework.model.core.ComponentReference;
import edu.mayo.cts2.framework.model.core.MatchAlgorithmReference;
import edu.mayo.cts2.framework.model.core.PredicateReference;
import edu.mayo.cts2.framework.model.core.SortCriteria;
import edu.mayo.cts2.framework.model.directory.DirectoryResult;
import edu.mayo.cts2.framework.model.exception.UnspecifiedCts2Exception;
import edu.mayo.cts2.framework.model.service.core.NameOrURI;
import edu.mayo.cts2.framework.model.valuesetdefinition.ValueSetDefinition;
import edu.mayo.cts2.framework.model.valuesetdefinition.ValueSetDefinitionDirectoryEntry;
import edu.mayo.cts2.framework.model.valuesetdefinition.ValueSetDefinitionListEntry;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ValueSetDefinitionSharedServiceBase;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ValueSetDefinitionQuery;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ValueSetDefinitionQueryService;

@Component("valueSetDefinitionQueryServiceImpl")
public class ValueSetDefinitionQueryServiceImpl extends ValueSetDefinitionSharedServiceBase implements ValueSetDefinitionQueryService
{
	@Override
	public DirectoryResult<ValueSetDefinitionDirectoryEntry> getResourceSummaries(ValueSetDefinitionQuery query, SortCriteria sortCriteria, Page page)
	{
		logger_.debug("getResourceSummaries {}, {}, {}", query, sortCriteria, page);
		// Note, this impl just lists everything, none of the parameters are currently handled.

		ArrayList<ValueSetDefinitionDirectoryEntry> results = new ArrayList<ValueSetDefinitionDirectoryEntry>();

		for (ValueSetDefinition vsd : ValueSetDefinitionStorage.getInstance().getAll())
		{
			ValueSetDefinitionDirectoryEntry vsdde = new ValueSetDefinitionDirectoryEntry();
			String localName = ValueSetDefinitionStorage.getInstance().getLocalName(vsd);
			vsdde.setResourceName(localName);
			vsdde.setAbout(vsd.getAbout());
			vsdde.setHref(utilities_.getUrlConstructor().createValueSetDefinitionUrl(
					(StringUtils.isBlank(vsd.getDefinedValueSet().getContent()) ? vsd.getDefinedValueSet().getUri() : vsd.getDefinedValueSet().getContent()), localName));
			vsdde.setDocumentURI(vsd.getDocumentURI());
			vsdde.setDefinedValueSet(vsd.getDefinedValueSet());
			vsdde.setFormalName(vsd.getFormalName());
			vsdde.setOfficialReleaseDate(vsd.getOfficialReleaseDate());
			vsdde.setOfficialResourceVersionId(vsd.getOfficialResourceVersionId());
			vsdde.setResourceSynopsis(vsd.getResourceSynopsis());
			vsdde.setVersionTag(vsd.getVersionTagAsReference());
			results.add(vsdde);
		}

		return new DirectoryResult<ValueSetDefinitionDirectoryEntry>(results, true);
	}

	@Override
	public DirectoryResult<ValueSetDefinitionListEntry> getResourceList(ValueSetDefinitionQuery query, SortCriteria sortCriteria, Page page)
	{
		logger_.debug("getResourceList {}, {}, {}", query, sortCriteria, page);
		// Note, this impl just lists everything, the only handled parameter to date is a valueSet restriction in the query
		ArrayList<ValueSetDefinitionListEntry> results = new ArrayList<ValueSetDefinitionListEntry>();

		// why the second list? - https://github.com/cts2/cts2-specification/issues/162
		for (ValueSetDefinition vsd : ValueSetDefinitionStorage.getInstance().getAll())
		{
			ValueSetDefinitionListEntry vsdde = new ValueSetDefinitionListEntry();
			vsdde.setHref(utilities_.getUrlConstructor().createValueSetDefinitionUrl(
					(StringUtils.isBlank(vsd.getDefinedValueSet().getContent()) ? vsd.getDefinedValueSet().getUri() : vsd.getDefinedValueSet().getContent()),
					ValueSetDefinitionStorage.getInstance().getLocalName(vsd)));
			vsdde.addEntry(vsd);
			if (passesRestrictions(vsd, query))
			{
				results.add(vsdde);
			}
		}
		return new DirectoryResult<ValueSetDefinitionListEntry>(results, true);
	}

	private boolean passesRestrictions(ValueSetDefinition vsd, ValueSetDefinitionQuery query)
	{
		if (query == null || query.getRestrictions() == null || query.getRestrictions().getValueSet() == null)
		{
			return true;
		}

		NameOrURI queryVS = query.getRestrictions().getValueSet();

		if (vsd.getAbout().equals(queryVS.getUri()) || ValueSetDefinitionStorage.getInstance().getLocalName(vsd).equals(queryVS.getName()))
		{
			return true;
		}

		return false;
	}

	@Override
	public int count(ValueSetDefinitionQuery query)
	{
		return ValueSetDefinitionStorage.getInstance().getAll().size();
	}

	@Override
	public Set<? extends MatchAlgorithmReference> getSupportedMatchAlgorithms()
	{
		throw new UnspecifiedCts2Exception("Unsupported operation");
	}

	@Override
	public Set<? extends ComponentReference> getSupportedSearchReferences()
	{
		throw new UnspecifiedCts2Exception("Unsupported operation");
	}

	@Override
	public Set<? extends ComponentReference> getSupportedSortReferences()
	{
		throw new UnspecifiedCts2Exception("Unsupported operation");
	}

	@Override
	public Set<PredicateReference> getKnownProperties()
	{
		throw new UnspecifiedCts2Exception("Unsupported operation");
	}

	@Override
	public String getServiceName()
	{
		return "Non-Persistent " + super.getServiceName();
	}
}
