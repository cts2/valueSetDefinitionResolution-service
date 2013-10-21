package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ctsUtility;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.mayo.cts2.framework.core.client.Cts2RestClient;
import edu.mayo.cts2.framework.core.timeout.Timeout;
import edu.mayo.cts2.framework.model.command.Page;
import edu.mayo.cts2.framework.model.command.ResolvedFilter;
import edu.mayo.cts2.framework.model.command.ResolvedReadContext;
import edu.mayo.cts2.framework.model.core.types.CompleteDirectory;
import edu.mayo.cts2.framework.model.directory.DirectoryResult;
import edu.mayo.cts2.framework.model.entity.EntityDirectory;
import edu.mayo.cts2.framework.model.entity.EntityDirectoryEntry;
import edu.mayo.cts2.framework.model.exception.UnspecifiedCts2Exception;
import edu.mayo.cts2.framework.model.service.core.NameOrURI;
import edu.mayo.cts2.framework.model.service.core.types.ActiveOrAll;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ctsUtility.queryBuilders.EntityDescriptionQueryBuilder;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.valueSetDefinitionResolutionImpl.utility.EntityReferenceResolver;
import edu.mayo.cts2.framework.service.profile.entitydescription.EntityDescriptionQuery;
import edu.mayo.cts2.framework.service.profile.entitydescription.EntityDescriptionQueryService;

/**
 * Utility code to handle iterating an entire terminology, and building up the entity objects I need for
 * ValueSetDefinition Resolution
 * 
 * @author darmbrust
 * 
 */


public class EntityIterator implements Iterator<EntityReferenceResolver>
{
	private Utilities utilities_;
	private final int readPageSize = 500;
	private ArrayList<EntityReferenceResolver> buffer_ = new ArrayList<>(500);
	private int pageId = 0;
	private boolean finished = false;
	private RuntimeException e;
	private String codeSystemVersionName, codeSystemName, entityHref, codeSystemServiceRootURL;
	private ResolvedFilter resolvedFilter_;
	private ResolvedReadContext readContext_;

	protected final Logger logger_ = LoggerFactory.getLogger(this.getClass());

	public EntityIterator(String codeSystemVersionName, String codeSystemName, String entityHref, String codeSystemServiceRootURL, ResolvedFilter resolvedFilter,
			ResolvedReadContext readContext, Utilities utilities)
	{
		this.utilities_ =  utilities;
		this.codeSystemVersionName = codeSystemVersionName;
		this.codeSystemName = codeSystemName;
		this.codeSystemServiceRootURL = codeSystemServiceRootURL;
		this.resolvedFilter_ = resolvedFilter;
		this.readContext_ = readContext;

		if (entityHref != null && entityHref.indexOf('?') > 0)
		{
			this.entityHref = entityHref.substring(0, entityHref.indexOf('?'));  // strip out any query parameters
		}
		getMore();
	}

	private void getMore()
	{
		if (finished)
		{
			return;
		}
		if (Timeout.isTimeLimitExceeded())
		{
			throw new RuntimeException("Notified of timeout");
		}
		EntityDescriptionQueryService edqs = utilities_.getLocalEntityDescriptionQueryService();
		List<EntityDirectoryEntry> results;
		if (edqs != null)
		{
			logger_.debug("Iterating entities with local service");
			Page page = new Page();
			page.setMaxToReturn(readPageSize);
			page.setPage(pageId++);

			EntityDescriptionQuery query = EntityDescriptionQueryBuilder.build(readContext_);

			NameOrURI codeSystem = new NameOrURI();
			codeSystem.setName(codeSystemVersionName);
			query.getRestrictions().getCodeSystemVersions().add(codeSystem);
			query.getReadContext().setActive(ActiveOrAll.ACTIVE_ONLY);

			if (resolvedFilter_ != null)
			{
				query.getFilterComponent().add(resolvedFilter_);
			}

			// TODO BUG list operation is broken at the moment - https://github.com/cts2/cts2-framework/issues/23
			// DirectoryResult<EntityListEntry> temp = edqs.getResourceList(null, null, page);
			// results = temp.getEntries();

			DirectoryResult<EntityDirectoryEntry> temp = edqs.getResourceSummaries(query, null, page);
			if (temp.isAtEnd())
			{
				finished = true;
			}
			results = temp.getEntries();
		}
		else if (StringUtils.isNotBlank(codeSystemServiceRootURL) || StringUtils.isNotBlank(entityHref))
		{
			String params = "?page=" + pageId++ + "&maxtoreturn=" + readPageSize + "&list=false&active=" + ActiveOrAll.ACTIVE_ONLY.toString();

			if (resolvedFilter_ != null)
			{
				params += "&matchalgorithm="
						+ (StringUtils.isEmpty(resolvedFilter_.getMatchAlgorithmReference().getUri()) ? resolvedFilter_.getMatchAlgorithmReference().getContent()
								: resolvedFilter_.getMatchAlgorithmReference().getUri());
				params += "&matchvalue=" + resolvedFilter_.getMatchValue();
				params += "&filtercomponent=";
				if (StringUtils.isNotBlank(resolvedFilter_.getComponentReference().getAttributeReference()))
				{
					params += resolvedFilter_.getComponentReference().getAttributeReference();
				}
				else if (StringUtils.isNotBlank(resolvedFilter_.getComponentReference().getSpecialReference()))
				{
					params += resolvedFilter_.getComponentReference().getSpecialReference();
				}
				else if (resolvedFilter_.getComponentReference().getPropertyReference() != null)
				{
					params += (StringUtils.isEmpty(resolvedFilter_.getComponentReference().getPropertyReference().getUri()) ? resolvedFilter_.getComponentReference()
							.getPropertyReference().getName() : resolvedFilter_.getComponentReference().getPropertyReference().getUri());
				}
			}

			params += utilities_.parameterizeReadContext(readContext_, false);

			EntityDirectory e;
			if (StringUtils.isNotBlank(codeSystemServiceRootURL))
			{
				logger_.debug("Iterating entities with remote service");
				// TODO BUG list=true is broken at the moment - https://github.com/cts2/cts2-framework/issues/23
				// EntityList e = Cts2RestClient.instance().getCts2Resource(
				// codeSystemServiceRootURL + (codeSystemServiceRootURL.endsWith("/") ? "" : "/") + "codesystem/" + codeSystemName + "/" + "/version/"
				// + codeSystemVersionName + "/entities?page=" + pageId++ + "&maxtoreturn=500&list=true", EntityList.class);

				e = Cts2RestClient.instance().getCts2Resource(
						codeSystemServiceRootURL + (codeSystemServiceRootURL.endsWith("/") ? "" : "/") + "codesystem/" + codeSystemName + "/version/"
								+ codeSystemVersionName + "/entities" + params, EntityDirectory.class);
			}
			else
			{
				logger_.debug("Iterating entities with provided href");
				e = Cts2RestClient.instance().getCts2Resource(entityHref + "?" + params, EntityDirectory.class);
			}

			if (e.getComplete().equals(CompleteDirectory.COMPLETE))
			{
				finished = true;
			}
			results = e.getEntryAsReference();
		}
		else
		{
			throw new UnspecifiedCts2Exception("No mechanism is available to resolve CodeSystem Entities");
		}

		for (EntityDirectoryEntry ede : results)
		{
			buffer_.add(new EntityReferenceResolver(ede));
		}
	}

	@Override
	public boolean hasNext()
	{
		if (e != null)
		{
			throw e;
		}
		if (buffer_.size() == 0)
		{
			getMore();
		}
		return buffer_.size() > 0;
	}

	@Override
	public EntityReferenceResolver next()
	{
		if (e != null)
		{
			throw e;
		}
		if (buffer_.size() == 0)
		{
			throw new NoSuchElementException();
		}
		return buffer_.remove(0);
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException();
	}
}
