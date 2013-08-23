package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices;

import java.security.InvalidParameterException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import javax.annotation.Resource;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import edu.mayo.cts2.framework.core.client.Cts2RestClient;
import edu.mayo.cts2.framework.core.config.ServerContext;
import edu.mayo.cts2.framework.core.constants.URIHelperInterface;
import edu.mayo.cts2.framework.core.url.UrlConstructor;
import edu.mayo.cts2.framework.core.xml.Cts2Marshaller;
import edu.mayo.cts2.framework.model.codesystemversion.CodeSystemVersionCatalogEntry;
import edu.mayo.cts2.framework.model.codesystemversion.CodeSystemVersionCatalogEntryDirectory;
import edu.mayo.cts2.framework.model.codesystemversion.CodeSystemVersionCatalogEntryMsg;
import edu.mayo.cts2.framework.model.codesystemversion.CodeSystemVersionCatalogEntrySummary;
import edu.mayo.cts2.framework.model.command.Page;
import edu.mayo.cts2.framework.model.command.ResolvedFilter;
import edu.mayo.cts2.framework.model.command.ResolvedReadContext;
import edu.mayo.cts2.framework.model.command.ReturnContentFilter.PropertyType;
import edu.mayo.cts2.framework.model.core.CodeSystemReference;
import edu.mayo.cts2.framework.model.core.CodeSystemVersionReference;
import edu.mayo.cts2.framework.model.core.NameAndMeaningReference;
import edu.mayo.cts2.framework.model.core.ScopedEntityName;
import edu.mayo.cts2.framework.model.core.URIAndEntityName;
import edu.mayo.cts2.framework.model.core.ValueSetDefinitionReference;
import edu.mayo.cts2.framework.model.core.ValueSetReference;
import edu.mayo.cts2.framework.model.core.VersionTagReference;
import edu.mayo.cts2.framework.model.core.types.CompleteDirectory;
import edu.mayo.cts2.framework.model.directory.DirectoryResult;
import edu.mayo.cts2.framework.model.entity.EntityReferenceMsg;
import edu.mayo.cts2.framework.model.extension.LocalIdValueSetDefinition;
import edu.mayo.cts2.framework.model.service.core.EntityNameOrURI;
import edu.mayo.cts2.framework.model.service.core.NameOrURI;
import edu.mayo.cts2.framework.model.service.core.types.ActiveOrAll;
import edu.mayo.cts2.framework.model.service.exception.UnknownValueSet;
import edu.mayo.cts2.framework.model.service.exception.UnknownValueSetDefinition;
import edu.mayo.cts2.framework.model.util.ModelUtils;
import edu.mayo.cts2.framework.model.valueset.ValueSetCatalogEntry;
import edu.mayo.cts2.framework.model.valuesetdefinition.ValueSetDefinitionDirectoryEntry;
import edu.mayo.cts2.framework.model.valuesetdefinition.ValueSetDefinitionMsg;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.queries.CodeSystemVersionQueryBuilder;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.queries.ValueSetDefinitionQueryBuilder;
import edu.mayo.cts2.framework.service.profile.association.AssociationQueryService;
import edu.mayo.cts2.framework.service.profile.codesystemversion.CodeSystemVersionQuery;
import edu.mayo.cts2.framework.service.profile.codesystemversion.CodeSystemVersionQueryService;
import edu.mayo.cts2.framework.service.profile.codesystemversion.CodeSystemVersionReadService;
import edu.mayo.cts2.framework.service.profile.entitydescription.EntityDescriptionQueryService;
import edu.mayo.cts2.framework.service.profile.entitydescription.EntityDescriptionReadService;
import edu.mayo.cts2.framework.service.profile.valueset.ValueSetReadService;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ValueSetDefinitionQuery;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ValueSetDefinitionQueryService;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ValueSetDefinitionReadService;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.name.ValueSetDefinitionReadId;
import edu.mayo.cts2.framework.service.provider.ServiceProvider;

@Component("valueSetDefinitionResolutionUtilities")
public class Utilities
{
	@Resource 
	static protected ServerContext serverContext_;

	@Resource 
	protected Cts2Marshaller cts2Marshaller_;

	@Resource 
	protected ServiceProvider valueSetDefinitionResolutionServiceProvider_;

	@Resource 
	protected ServiceProvider valueSetDefinitionServiceProvider_;

	@Resource 
	protected ServiceProvider valueSetServiceProvider_;

	@Value("#{configProperties.valueSetServiceRootURL}") 
	protected String valueSetServiceRootURL_;

	@Value("#{configProperties.valueSetDefinitionServiceRootURL}") 
	protected String valueSetDefinitionServiceRootURL_;

	@Value("#{configProperties.codeSystemAndEntitiesServicesRootURL}") 
	protected String codeSystemAndEntitiesServicesRootURL_;

	static private UrlConstructor urlConstructor_;

	private Cts2RestClient cts2RestClient_;

//	private static Cache<String, CodeSystemVersionReference> namespaceToCodeSystemVersionCache = CacheBuilder.newBuilder().concurrencyLevel(4).maximumSize(10)
//			.expireAfterWrite(5, TimeUnit.MINUTES).build();

	protected final Logger logger_ = LoggerFactory.getLogger(this.getClass());

	public void updateValueSetForReturn(ValueSetCatalogEntry valueSetCatalogEntry, String valueSetHref, ResolvedReadContext readContext)
	{
		logger_.debug("updateValueSetForReturn {} {}", valueSetCatalogEntry, valueSetHref);
		valueSetCatalogEntry.setCurrentDefinition(lookupCurrentValueSetDefinitionReference(valueSetCatalogEntry.getAbout(), valueSetHref,
				valueSetCatalogEntry.getValueSetName(), readContext));
		valueSetCatalogEntry.setDefinitions(getUrlConstructor().createValueSetUrl(valueSetCatalogEntry.getValueSetName()) + "/definitions");
	}

	public ValueSetDefinitionReference lookupCurrentValueSetDefinitionReference(final String valueSetURI, String valueSetHref, String valueSetName,
			final ResolvedReadContext readContext)
	{
		logger_.debug("lookupCurrentValueSetDefinitionReference for  valueSetURI {} valueSetHref {} valueSetName{}", valueSetURI, valueSetHref, valueSetName);

		// we will assume that the ValueSetDefininitionResolutionService is on this system - otherwise, they probably won't be using these stub
		// implementations of the ValueSet code - no need to look for an external service.
		ValueSetDefinitionQueryService vsdqr = valueSetDefinitionResolutionServiceProvider_.getService(ValueSetDefinitionQueryService.class);

		ValueSetDefinitionQuery vsdQuery = ValueSetDefinitionQueryBuilder.build(readContext);
		NameOrURI valueSet = new NameOrURI();
		valueSet.setUri(valueSetURI);
		vsdQuery.getRestrictions().setValueSet(valueSet);
		
		DirectoryResult<ValueSetDefinitionDirectoryEntry> definitions = vsdqr.getResourceSummaries(vsdQuery, null, null);
		logger_.debug("Query returned {} results", definitions.getEntries().size());

		ValueSetDefinitionDirectoryEntry vsdde = null;
		for (ValueSetDefinitionDirectoryEntry temp : definitions.getEntries())
		{
			if (vsdde != null)
			{
				break;
			}
			for (VersionTagReference version : temp.getVersionTag())
			{
				if (version.getContent().equals(URIHelperInterface.DEFAULT_TAG))
				{
					vsdde = temp;
					break;
				}
			}
		}

		if (vsdde != null)
		{
			ValueSetDefinitionReference vsdr = new ValueSetDefinitionReference();

			ValueSetReference valueSetReference = new ValueSetReference();
			valueSetReference.setHref(valueSetHref);
			valueSetReference.setUri(valueSetURI);
			valueSetReference.setContent(valueSetName);
			vsdr.setValueSet(valueSetReference);

			NameAndMeaningReference valueSetDefinition = new NameAndMeaningReference();
			valueSetDefinition.setContent(vsdde.getResourceName());
			valueSetDefinition.setHref(vsdde.getHref());
			valueSetDefinition.setUri(vsdde.getAbout());

			vsdr.setValueSetDefinition(valueSetDefinition);
			return vsdr;
		}

		logger_.debug("No results matched " + URIHelperInterface.DEFAULT_TAG + " - returning null");

		return null;
	}

	public static UrlConstructor getUrlConstructor()
	{
		if (urlConstructor_ == null)
		{
			urlConstructor_ = new UrlConstructor(serverContext_);
		}
		return urlConstructor_;
	}

	public Cts2RestClient getRestClient()
	{
		if (cts2RestClient_ == null)
		{
			cts2RestClient_ = new Cts2RestClient(cts2Marshaller_, true);
		}
		return cts2RestClient_;
	}

	protected ValueSetCatalogEntry lookupValueSetByURI(String uri, ResolvedReadContext readContext)
	{
		logger_.debug("Looking up ValueSet by URI to get localname");

		ValueSetCatalogEntry vs = null;

		try
		{
			if (StringUtils.isNotBlank(valueSetServiceRootURL_))
			{
				logger_.debug("Looking up valueset using external server '{}'", valueSetServiceRootURL_);
				String address = valueSetServiceRootURL_ + (valueSetServiceRootURL_.endsWith("/") ? "" : "/") + "valuesetbyuri?uri=" + uri
						+ parameterizeReadContext(readContext, false);
				vs = getRestClient().getCts2Resource(address, ValueSetCatalogEntry.class);
			}
			else
			{
				logger_.debug("Looking up valueset using internal service");
				ValueSetReadService vssp = valueSetServiceProvider_.getService(ValueSetReadService.class);
				NameOrURI nameOrURI = new NameOrURI();
				nameOrURI.setUri(uri);
				vs = vssp.read(nameOrURI, readContext);
			}
		}
		catch (Exception e)
		{
			logger_.error("Lookup failed", e);
			throw new UnknownValueSet();
		}
		if (vs == null)
		{
			throw new UnknownValueSet();
		}
		return vs;
	}

	protected ValueSetCatalogEntry lookupValueSetByHref(String href)
	{
		logger_.debug("Looking up ValueSet by href to get URI and localName");
		try
		{
			ValueSetCatalogEntry vs = getRestClient().getCts2Resource(href, ValueSetCatalogEntry.class);
			if (vs == null)
			{
				throw new UnknownValueSet();
			}
			return vs;
		}
		catch (Exception e)
		{
			logger_.error("Lookup failed", e);
			throw new UnknownValueSet();
		}
	}

	protected ValueSetCatalogEntry lookupValueSetByLocalName(String localName, ResolvedReadContext readContext)
	{
		logger_.debug("Looking up ValueSet by Name to get URI");
		ValueSetCatalogEntry vs = null;

		try
		{
			if (StringUtils.isNotBlank(valueSetServiceRootURL_))
			{
				logger_.debug("Looking up valueset using external server '{}'", valueSetServiceRootURL_);
				String address = valueSetServiceRootURL_ + (valueSetServiceRootURL_.endsWith("/") ? "" : "/") + "valueset/" + localName
						+ parameterizeReadContext(readContext, true);
				vs = getRestClient().getCts2Resource(address, ValueSetCatalogEntry.class);
			}
			else
			{
				logger_.debug("Looking up valueset using internal service");
				ValueSetReadService vssp = valueSetServiceProvider_.getService(ValueSetReadService.class);

				NameOrURI nameOrURI = new NameOrURI();
				nameOrURI.setName(localName);
				vs = vssp.read(nameOrURI, readContext);
			}
		}
		catch (Exception e)
		{
			logger_.error("Lookup failed", e);
			throw new UnknownValueSet();
		}
		if (vs == null)
		{
			throw new UnknownValueSet();
		}
		return vs;
	}

	/**
	 * Uses the parameters from left to right - if uri is provided, uses that - otherwise, moves on to localname, then href.
	 * A lookup failure on any provided value is immediately returned (we don't try to look up again with another provided key)
	 * 
	 * @param uri 1st
	 * @param localName 2nd
	 * @param href 3rd
	 * @return
	 */
	protected ValueSetCatalogEntry lookupValueSetByAny(String uri, String localName, String href, ResolvedReadContext readContext)
	{
		if (StringUtils.isNotBlank(uri))
		{
			return lookupValueSetByURI(uri, readContext);
		}
		else if (StringUtils.isNotBlank(localName))
		{
			return lookupValueSetByLocalName(localName, readContext);
		}
		else if (StringUtils.isNotBlank(href))
		{
			return lookupValueSetByHref(href + parameterizeReadContext(readContext, href.indexOf('?') < 0));
		}
		else
		{
			throw new UnknownValueSet();
		}
	}

	public LocalIdValueSetDefinition lookupValueSetDefinition(ValueSetDefinitionReadId definitionId, ResolvedReadContext readContext)
	{
		logger_.debug("Looking up ValueSetDefinition '{}'", definitionId);

		LocalIdValueSetDefinition definition = null;
		try
		{
			if (StringUtils.isBlank(valueSetDefinitionServiceRootURL_))
			{
				logger_.debug("Looking up via local service");
				ValueSetDefinitionReadService vsdrs = valueSetDefinitionServiceProvider_.getService(ValueSetDefinitionReadService.class);
				definition = vsdrs.read(definitionId, readContext);
			}
			else
			{
				String address = valueSetDefinitionServiceRootURL_ + (valueSetDefinitionServiceRootURL_.endsWith("/") ? "" : "/");
				if (StringUtils.isNotBlank(definitionId.getUri()))
				{
					logger_.debug("Looking up via URI on remote service");
					address += "valuesetdefinitionbyuri?uri=" + definitionId.getUri();
					address += parameterizeReadContext(readContext, false);
				}
				else if (StringUtils.isNotBlank(definitionId.getName()) && definitionId.getValueSet() != null
						&& (StringUtils.isNotBlank(definitionId.getValueSet().getName()) || StringUtils.isNotBlank(definitionId.getValueSet().getUri())))
				{
					if (StringUtils.isNotBlank(definitionId.getValueSet().getName()))
					{
						logger_.debug("Looking up via Name/Name on remote service");
						address += "valueset/" + definitionId.getValueSet().getName() + "/definition/" + definitionId.getName();
						address += parameterizeReadContext(readContext, true);
					}
					else
					{
						logger_.debug("Looking up via Name/URI on remote service ");
						address += "valuesetbyuri/definition/" + definitionId.getName() + "?uri=" + definitionId.getValueSet().getUri();
						address += parameterizeReadContext(readContext, false);
					}
				}
				else
				{
					throw new UnknownValueSetDefinition();
				}
				ValueSetDefinitionMsg temp = getRestClient().getCts2Resource(address, ValueSetDefinitionMsg.class);
				definition = new LocalIdValueSetDefinition(temp.getValueSetDefinition());

				String rr = temp.getHeading().getResourceRoot();  // Looks like valueset/AdministrativeGender/definition/1 - need to parse out the end
				if (rr.indexOf('/') > 0)
				{
					definition.setLocalID(rr.substring(rr.lastIndexOf('/'), rr.length()));
				}
				else
				{
					throw new RuntimeException("failed to parse out the localId");
				}
				return definition;
			}
		}
		catch (UnknownValueSetDefinition e)
		{
			throw e;
		}
		catch (Exception e)
		{
			logger_.error("Unable to lookup requested ValueSetDefinition", e);
			throw new UnknownValueSetDefinition();
		}

		if (definition == null)
		{
			throw new UnknownValueSetDefinition();
		}
		return definition;
	}

	public static String parameterizeReadContext(ResolvedReadContext readContext, boolean isFirstParam)
	{
		if (readContext == null)
		{
			return "";
		}
		StringBuilder sb = new StringBuilder();
		if (readContext.getActive() != null)
		{
			sb.append("&active=");
			sb.append(readContext.getActive().toString());
		}
		if (StringUtils.isNotBlank(readContext.getChangeSetContextUri()))
		{
			sb.append("&changesetcontext=");
			sb.append(readContext.getChangeSetContextUri());
		}
		if (readContext.getLanguageReference() != null && StringUtils.isNotBlank(readContext.getLanguageReference().getContent()))
		{
			sb.append("&referencelanguage=");
			sb.append(readContext.getLanguageReference().getContent());
		}
		if (readContext.getReferenceTime() != null)
		{
			sb.append("&referencetime=");

			Calendar c = GregorianCalendar.getInstance();
			c.setTime(readContext.getReferenceTime());
			sb.append(javax.xml.bind.DatatypeConverter.printDateTime(c));
		}
		if (readContext.getReturnContentFilter() != null)
		{
			for (PropertyType pt : readContext.getReturnContentFilter().getPropertyTypes())
			{
				sb.append("&filtercomponent=");
				sb.append(pt.toString());
			}
		}
		if (sb.length() > 0 && isFirstParam)
		{
			sb.replace(0, 1, "?");
		}
		return sb.toString();
	}

	public EntityReferenceAndHref resolveEntityReference(URIAndEntityName entity, ResolvedReadContext readContext)
	{
		readContext.setActive(ActiveOrAll.ACTIVE_ONLY);
		try
		{
			EntityDescriptionReadService edrs = getLocalEntityDescriptionReadService();
			if (edrs != null)
			{
				logger_.debug("resolving up EntityReference using local EntityDescriptionService");
				EntityNameOrURI e = new EntityNameOrURI();
				
				if (StringUtils.isNotBlank(entity.getUri()))
				{
					e.setUri(entity.getUri());
				}
				//This shouldn't happen if they are following spec (URI is required) but be nice anyway...
				else if (StringUtils.isNotBlank(entity.getName()) && StringUtils.isNotBlank(entity.getNamespace()))
				{
					ScopedEntityName entityName = new ScopedEntityName();
					entityName.setName(entity.getName());
					entityName.setNamespace(entity.getNamespace());
					e.setEntityName(entityName);
				}
				else
				{
					throw new InvalidParameterException("URI is required within a  URIAndEntityName!");
				}
				return new EntityReferenceAndHref(edrs.availableDescriptions(e, readContext));
			}
			else if (StringUtils.isNotBlank(codeSystemAndEntitiesServicesRootURL_))
			{
				logger_.debug("resolving up EntityReference using remote EntityDescriptionService");
				
				String url = null;
				
				if (StringUtils.isNotBlank(entity.getUri()))
				{
					url = "entitybyuri?uri=" + entity.getUri() + "&";
				}
				//This shouldn't happen if they are following spec (URI is required) but be nice anyway...
				else if (StringUtils.isNotBlank(entity.getName()) && StringUtils.isNotBlank(entity.getNamespace()))
				{
					url = "entity/" + entity.getNamespace() + ":" + entity.getName() + "?";
				}
				else
				{
					throw new InvalidParameterException("URI is required within a  URIAndEntityName!");
				}
				return new EntityReferenceAndHref(getRestClient().getCts2Resource(
						codeSystemAndEntitiesServicesRootURL_ + (codeSystemAndEntitiesServicesRootURL_.endsWith("/") ? "" : "/") + url + "&list=false"
								+ parameterizeReadContext(readContext, false), EntityReferenceMsg.class));
			}
			else
			{
				logger_.debug("No locally configured service is available to lookup EntityDescription by URI");
			}
		}
		catch (Exception e)
		{
			logger_.warn("Error looking up EntityDesignation", e);
		}

		//Our local service didn't work, but perhaps, if there is an href on the entity - that will work.  Give it a try.
		if (StringUtils.isNotBlank(entity.getHref()))
		{
			logger_.debug("resolving up EntityReference using provided href");
			try
			{
				String uriTemp;
				if (entity.getHref().indexOf("?") > 0)
				{
					uriTemp = entity.getHref().substring(0, entity.getHref().indexOf('?'));
				}
				else
				{
					uriTemp = entity.getHref();
				}
				uriTemp += parameterizeReadContext(readContext, false);

				return new EntityReferenceAndHref(getRestClient().getCts2Resource(uriTemp, EntityReferenceMsg.class));
			}
			catch (Exception e)
			{
				logger_.debug("Error looking up EntityDesignation by provided href", e);
			}
		}
		return null;
	}

	protected static EntityDescriptionReadService getLocalEntityDescriptionReadService()
	{
		// ServiceProvider serviceProvider = serviceProviderFactory.getServiceProvider();
		// return serviceProvider.getService(EntityDescriptionReadService.class);
		return null;// TODO fix local service lookup
	}

	protected static EntityDescriptionQueryService getLocalEntityDescriptionQueryService()
	{
		// ServiceProvider serviceProvider = serviceProviderFactory.getServiceProvider();
		// return serviceProvider.getService(EntityDescriptionQueryService.class);
		return null;// TODO fix local service lookup
	}

	protected static CodeSystemVersionReadService getLocalCodeSystemVersionReadService()
	{
		// ServiceProvider serviceProvider = serviceProviderFactory.getServiceProvider();
		// return serviceProvider.getService(CodeSystemVersionReadService.class);
		return null;// TODO fix local service lookup
	}

	protected static CodeSystemVersionQueryService getLocalCodeSystemVersionQueryService()
	{
		// ServiceProvider serviceProvider = serviceProviderFactory.getServiceProvider();
		// return serviceProvider.getService(CodeSystemVersionQueryService.class);
		return null;// TODO fix local service lookup
	}

	public static AssociationQueryService getLocalAssociationQueryService()
	{
		// ServiceProvider serviceProvider = serviceProviderFactory.getServiceProvider();
		// return serviceProvider.getService(AssociationQueryService.class);
		return null;// TODO fix local service lookup
	}

	public CodeSystemVersionCatalogEntry lookupCodeSystemVersion(CodeSystemReference codeSystem, CodeSystemVersionReference codeSystemVersion,
			ResolvedReadContext readContext)
	{
		CodeSystemVersionReadService csvrs = getLocalCodeSystemVersionReadService();
		CodeSystemVersionCatalogEntry result = null;

		if (codeSystem == null)
		{
			throw new IllegalArgumentException("CodeSystem is required");
		}

		if (codeSystemVersion != null && codeSystemVersion.getVersion() != null)
		{
			if (StringUtils.isNotBlank(codeSystemVersion.getVersion().getUri()))
			{
				// This URI by itself is enough to do the lookup.
				if (csvrs != null)
				{
					logger_.debug("Looking up CodeSystemVersion using local CodeSystemVersionService");
					NameOrURI nou = new NameOrURI();
					nou.setUri(codeSystemVersion.getVersion().getUri());
					result = csvrs.read(nou, readContext);
				}
				else if (StringUtils.isNotBlank(codeSystemAndEntitiesServicesRootURL_))
				{
					logger_.debug("Looking up CodeSystemVersion using remote CodeSystemVersionService");
					result = getRestClient().getCts2Resource(
							codeSystemAndEntitiesServicesRootURL_ + (codeSystemAndEntitiesServicesRootURL_.endsWith("/") ? "" : "/") + "codesystemversionbyuri?uri="
									+ codeSystemVersion.getVersion().getUri() + parameterizeReadContext(readContext, false), CodeSystemVersionCatalogEntryMsg.class)
							.getCodeSystemVersionCatalogEntry();
				}
			}
			else if (StringUtils.isNotBlank(codeSystemVersion.getVersion().getContent()))
			{
				// Have a codeSystemVersionName - need to combine this with a codeSystem name or URI
				String codeSystemVersionName = codeSystemVersion.getVersion().getContent();
				if (StringUtils.isNotBlank(codeSystemVersion.getCodeSystem().getUri()))
				{
					if (csvrs != null)
					{
						logger_.debug("Looking up CodeSystemVersion using local CodeSystemVersionService");
						result = csvrs.getCodeSystemByVersionId(ModelUtils.nameOrUriFromUri(codeSystemVersion.getCodeSystem().getUri()), codeSystemVersionName,
								readContext);
					}
					else if (StringUtils.isNotBlank(codeSystemAndEntitiesServicesRootURL_))
					{
						logger_.debug("Looking up CodeSystemVersion using remote CodeSystemVersionService");
						result = getRestClient().getCts2Resource(
								codeSystemAndEntitiesServicesRootURL_ + (codeSystemAndEntitiesServicesRootURL_.endsWith("/") ? "" : "/") + "codesystembyuri/version"
										+ codeSystemVersionName + "?uri=" + codeSystemVersion.getCodeSystem().getUri() + parameterizeReadContext(readContext, false),
								CodeSystemVersionCatalogEntryMsg.class).getCodeSystemVersionCatalogEntry();
					}
				}
				else if (StringUtils.isNotBlank(codeSystemVersion.getCodeSystem().getContent()))
				{
					if (csvrs != null)
					{
						logger_.debug("Looking up CodeSystemVersion using local CodeSystemVersionService");
						result = csvrs.getCodeSystemByVersionId(ModelUtils.nameOrUriFromName(codeSystemVersion.getCodeSystem().getContent()), codeSystemVersionName,
								readContext);
					}
					else if (StringUtils.isNotBlank(codeSystemAndEntitiesServicesRootURL_))
					{
						result = getRestClient().getCts2Resource(
								codeSystemAndEntitiesServicesRootURL_ + (codeSystemAndEntitiesServicesRootURL_.endsWith("/") ? "" : "/") + "codesystem/"
										+ codeSystemVersion.getCodeSystem().getContent() + "/version/" + codeSystemVersionName
										+ parameterizeReadContext(readContext, true), CodeSystemVersionCatalogEntryMsg.class).getCodeSystemVersionCatalogEntry();
					}
				}
			}

			// If we get here, and we have an href, try it.
			if (result == null && StringUtils.isNotBlank(codeSystemVersion.getVersion().getHref()))
			{
				// Have an href, try that - if it works, should be enough by itself
				logger_.debug("Looking up CodeSystemVersion using supplied HREF");
				String url = codeSystemVersion.getVersion().getHref();

				url += parameterizeReadContext(readContext, url.indexOf('?') < 0);

				result = getRestClient().getCts2Resource(url, CodeSystemVersionCatalogEntryMsg.class).getCodeSystemVersionCatalogEntry();
			}
		}

		// This fills in any missing params, and return the service default version
		CodeSystemVersionCatalogEntrySummary serviceDefaultVersion = getDefaultCodeSystemVersion(codeSystem, readContext);

		if (result != null)
		{
			if (!result.getVersionOf().getUri().equals(serviceDefaultVersion.getVersionOf().getUri()))
			{
				throw new IllegalArgumentException("The specified CodeSystemVersion does not match the specified CodeSystem");
			}
			return result;
		}

		// Turn around, and look up the version object from our entrySummary

		CodeSystemVersionReference temp = new CodeSystemVersionReference();
		temp.setCodeSystem(serviceDefaultVersion.getVersionOf());
		NameAndMeaningReference namr = new NameAndMeaningReference();
		namr.setContent(serviceDefaultVersion.getCodeSystemVersionName());
		namr.setHref(serviceDefaultVersion.getHref());
		namr.setUri(serviceDefaultVersion.getAbout());
		temp.setVersion(namr);
		return lookupCodeSystemVersion(serviceDefaultVersion.getVersionOf(), temp, readContext);
	}

	/**
	 * @param codeSystem
	 * @return
	 */
	public CodeSystemVersionCatalogEntrySummary getDefaultCodeSystemVersion(CodeSystemReference codeSystem, ResolvedReadContext readContext)
	{
		// Lookup up the codeSystem parameter (which is required), then check equality on the version (if the version was passed in)
		CodeSystemVersionQueryService csvqs = getLocalCodeSystemVersionQueryService();

		CodeSystemVersionCatalogEntrySummary finalResult = null;
		CodeSystemVersionCatalogEntrySummary potentialResult = null;

		if (csvqs != null)
		{
			if (StringUtils.isNotBlank(codeSystem.getUri()) || StringUtils.isNotBlank(codeSystem.getContent()))
			{
				logger_.debug("Looking up a code system using the local service");
				int pageNumber = 0;

				CodeSystemVersionQuery query = CodeSystemVersionQueryBuilder.build(readContext);
				NameOrURI csInfo = new NameOrURI();
				csInfo.setUri(codeSystem.getUri());
				csInfo.setName(codeSystem.getContent());
				query.getRestrictions().setCodeSystem(csInfo);

				while (true)
				{
					if (finalResult != null)
					{
						break;
					}
					Page page = new Page();
					page.setMaxToReturn(500);
					page.setPage(pageNumber++);

					DirectoryResult<CodeSystemVersionCatalogEntrySummary> result = csvqs.getResourceSummaries(query, null, page);
					for (CodeSystemVersionCatalogEntrySummary entry : result.getEntries())
					{
						if (finalResult != null)
						{
							break;
						}
						if (potentialResult == null)
						{
							potentialResult = entry;
						}
						for (VersionTagReference tag : entry.getCodeSystemVersionTagAsReference())
						{
							if ("PRODUCTION".equals(tag.getContent()) || "DEFAULT".equals(tag.getContent()))
							{
								finalResult = entry;
								break;
							}
						}
					}
					if (result.isAtEnd())
					{
						break;
					}
				}

			}
		}
		else
		{
			String queryUrl = null;
			if (StringUtils.isNotBlank(codeSystem.getUri()))
			{
				queryUrl = "codesystembyuri/versions?uri=" + codeSystem.getUri() + "&page=";
			}
			else if (StringUtils.isNotBlank(codeSystem.getContent()))
			{
				queryUrl = "codesystem/" + codeSystem.getContent() + "/versions?page=";
			}

			if (queryUrl != null && StringUtils.isNotBlank(codeSystemAndEntitiesServicesRootURL_))
			{
				int page = 0;
				while (true)
				{
					CodeSystemVersionCatalogEntryDirectory directory = getRestClient().getCts2Resource(
							codeSystemAndEntitiesServicesRootURL_ + (codeSystemAndEntitiesServicesRootURL_.endsWith("/") ? "" : "/") + queryUrl + page++
									+ parameterizeReadContext(readContext, false), CodeSystemVersionCatalogEntryDirectory.class);

					for (CodeSystemVersionCatalogEntrySummary entry : directory.getEntryAsReference())
					{
						if (finalResult != null)
						{
							break;
						}
						if (potentialResult == null)
						{
							potentialResult = entry;
						}
						for (VersionTagReference tag : entry.getCodeSystemVersionTagAsReference())
						{
							if ("PRODUCTION".equals(tag.getContent()) || "DEFAULT".equals(tag.getContent()))
							{
								finalResult = entry;
								break;
							}
						}
					}
					if (finalResult != null || CompleteDirectory.COMPLETE == directory.getComplete() || StringUtils.isBlank(directory.getNext()))
					{
						break;
					}
				}
			}

			// Nothing yet... lets try the server from the href...
			if (finalResult == null && queryUrl != null && StringUtils.isNotBlank(codeSystem.getHref()) && codeSystem.getHref().toLowerCase().indexOf("/codesystem") > 0)
			{
				int page = 0;

				String server = codeSystem.getHref().substring(0, codeSystem.getHref().toLowerCase().indexOf("/codesystem"));

				while (true)
				{
					CodeSystemVersionCatalogEntryDirectory directory = getRestClient().getCts2Resource(
							server + "/" + queryUrl + page++ + parameterizeReadContext(readContext, false), CodeSystemVersionCatalogEntryDirectory.class);

					for (CodeSystemVersionCatalogEntrySummary entry : directory.getEntryAsReference())
					{
						if (finalResult != null)
						{
							break;
						}
						if (potentialResult == null)
						{
							potentialResult = entry;
						}
						for (VersionTagReference tag : entry.getCodeSystemVersionTagAsReference())
						{
							if ("PRODUCTION".equals(tag.getContent()) || "DEFAULT".equals(tag.getContent()))
							{
								finalResult = entry;
								break;
							}
						}
					}
					if (finalResult != null || CompleteDirectory.COMPLETE == directory.getComplete() || StringUtils.isBlank(directory.getNext()))
					{
						break;
					}
				}
			}
		}

		if (finalResult != null)
		{
			return finalResult;
		}
		if (potentialResult != null)
		{
			return potentialResult;
		}
		else
		{
			throw new IllegalArgumentException("You must provide valid CodeSystem parameters");
		}
	}

	public Iterator<EntityReferenceResolver> getEntities(String codeSystemVersionName, String codeSystemName, String entityHref, ResolvedFilter resolvedFilter,
			ResolvedReadContext readContext)
	{
		return new EntityIterator(codeSystemVersionName, codeSystemName, entityHref, codeSystemAndEntitiesServicesRootURL_, resolvedFilter, readContext);
	}

	//TODO toss this?
//	protected CodeSystemVersionReference getCodeSystemVersionForEntity(final URIAndEntityName entity, final ResolvedReadContext readContext)
//	{
//		try
//		{
//			return namespaceToCodeSystemVersionCache.get(entity.getNamespace(), new Callable<CodeSystemVersionReference>()
//			{
//				@Override
//				public CodeSystemVersionReference call() throws Exception
//				{
//					EntityReference er = resolveEntityReference(entity, readContext);
//					return er.getKnownEntityDescriptionAsReference().get(0).getDescribingCodeSystemVersion();
//				}
//			});
//		}
//		catch (Exception e)
//		{
//			throw new RuntimeException("Error during code system version lookup for entity", e);
//		}
//	}

	public String makeAssociationURL(String codeSystemName, String codeSystemVersionName, String entityName, boolean sourceToTarget, String altServiceURL,
			ResolvedReadContext readContext)
	{
		String href = null;
		if (sourceToTarget)
		{
			href = getUrlConstructor().createSubjectOfUrl(codeSystemName, codeSystemVersionName, entityName);
		}
		else
		{
			href = getUrlConstructor().createTargetOfUrl(codeSystemName, codeSystemVersionName, entityName);
		}

		String temp = href.substring(href.indexOf("/codesystem/") + 1, href.length());
		temp += parameterizeReadContext(readContext, true);

		if (StringUtils.isBlank(codeSystemAndEntitiesServicesRootURL_))
		{
			if (StringUtils.isBlank(altServiceURL))
			{
				return null;
			}
			else
			{
				// Just want the server name and port from the altServiceURL
				int i = altServiceURL.toLowerCase().indexOf("/codesystem");  // this breaks if the server name is http://codesystem....
				if (i > 0)
				{
					return altServiceURL.substring(0, i) + "/" + temp;
				}
				else
				{
					return null;
				}
			}
		}
		else
		{
			return codeSystemAndEntitiesServicesRootURL_ + (codeSystemAndEntitiesServicesRootURL_.endsWith("/") ? "" : "/") + temp;
		}
	}
}
