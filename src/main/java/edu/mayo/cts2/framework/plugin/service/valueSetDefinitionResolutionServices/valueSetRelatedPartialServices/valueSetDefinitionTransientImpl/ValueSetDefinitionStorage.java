package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.valueSetRelatedPartialServices.valueSetDefinitionTransientImpl;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.mayo.cts2.framework.model.exception.UnspecifiedCts2Exception;
import edu.mayo.cts2.framework.model.extension.LocalIdValueSetDefinition;
import edu.mayo.cts2.framework.model.service.exception.DuplicateValueSetURI;
import edu.mayo.cts2.framework.model.service.exception.UnknownValueSet;
import edu.mayo.cts2.framework.model.valueset.ValueSetCatalogEntry;
import edu.mayo.cts2.framework.model.valuesetdefinition.ValueSetDefinition;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ctsUtility.ExceptionBuilder;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ctsUtility.Utilities;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.name.ValueSetDefinitionReadId;

public class ValueSetDefinitionStorage
{
	private static volatile ValueSetDefinitionStorage vsds_;
	
	// ValueSetDefinitionURI to ValueSetDefinition
	private HashMap<String, ValueSetDefinitionWithLocalName> valueSets_ = new HashMap<String, ValueSetDefinitionWithLocalName>();

	// Contains both of these mappings:
	// ValueSetDefinitionName:ValueSetName<==>ValueSetDefinitionURI
	// ValueSetDefinitionName:ValueSetURI<==>ValueSetDefinitionURI
	private HashMap<String, String> altNameHashes_ = new HashMap<String, String>();
	private AtomicInteger vsId = new AtomicInteger(1);
	private final Logger logger_ = LoggerFactory.getLogger(this.getClass());

	public static ValueSetDefinitionStorage getInstance()
	{
		if (vsds_ == null)
		{
			synchronized (ValueSetDefinitionStorage.class)
			{
				if (vsds_ == null)
				{
					vsds_ = new ValueSetDefinitionStorage();
				}
			}
		}
		return vsds_;
	}

	private ValueSetDefinitionStorage()
	{
		// just enforcing singleton
	}

	protected String getLocalName(ValueSetDefinition vsd)
	{
		logger_.debug("Get local name for ValueSetDefinition {}", vsd.getAbout());
		return valueSets_.get(vsd.getAbout()).localName_;
	}

	protected String getLocalNameForValueSet(String valueSetURI)
	{
		ValueSetDefinitionWithLocalName vsd = valueSets_.get(valueSetURI);
		if (vsd == null)
		{
			return null;
		}
		else
		{
			return vsd.localName_;
		}
	}

	protected ValueSetDefinition get(ValueSetDefinitionReadId id)
	{
		logger_.debug("get {}", id);

		String key = null;
		if (StringUtils.isNotBlank(id.getUri()))
		{
			// This will do...
			key = id.getUri();
		}
		else if (StringUtils.isNotBlank(id.getName()))
		{
			// With just a valuesetDefinitionName, - we need also need part of the ValueSet identifier
			if (StringUtils.isNotBlank(id.getValueSet().getUri()))
			{
				key = altNameHashes_.get(id.getName() + ":" + id.getValueSet().getUri());
			}
			else if (StringUtils.isNotBlank(id.getValueSet().getName()))
			{
				key = altNameHashes_.get(id.getName() + ":" + id.getValueSet().getName());
			}
			else
			{
				throw new UnspecifiedCts2Exception("When ValueSetDefinition 'name' is specified, a ValueSet 'name' or 'uri' must also be specified");
			}
		}
		else
		{
			throw ExceptionBuilder.buildUnknownValueSetReference("One of ValueSetDefinition 'name' or 'uri' must be specified");
		}

		ValueSetDefinitionWithLocalName vs = valueSets_.get(key);

		if (vs == null)
		{
			throw ExceptionBuilder.buildUnknownValueSetReference("Valueset was not found");
		}
		return vs.vsd_;
	}

	protected Collection<ValueSetDefinition> getAll()
	{
		ArrayList<ValueSetDefinition> result = new ArrayList<ValueSetDefinition>(valueSets_.size());
		for (ValueSetDefinitionWithLocalName vsdwn : valueSets_.values())
		{
			result.add(vsdwn.vsd_);
		}
		return result;
	}

	protected LocalIdValueSetDefinition store(ValueSetDefinition vsd, Utilities utilities)
	{
		String uri = vsd.getAbout();

		if (StringUtils.isBlank(uri))
		{
			throw new UnspecifiedCts2Exception("The URI for the ValueSetDefinition (About field) must be supplied");
		}

		if (valueSets_.containsKey(uri))
		{
			throw new DuplicateValueSetURI();  // TODO BUG should be 'definition' https://github.com/cts2/cts2-framework/issues/27
		}

		if (vsd.getDefinedValueSet() == null)
		{
			throw new UnspecifiedCts2Exception("The 'DefinedValueSet' must be specified");
		}

		try
		{
			new URI(uri);
		}
		catch (URISyntaxException e)
		{
			throw new UnspecifiedCts2Exception("The ValueSetDefinition 'About' field must be a valid URI - " + e);
		}

		// Bit tricky - we need to get both of these variables. But they don't (necessarily) have to exist in a ValueSet service.
		// If they provide both, just use as they are. If they only provide one, use it to look up the other. If they provide neither,
		// try using the HREF to look it up (if provided)

		String valueSetLocalName = vsd.getDefinedValueSet().getContent();
		String valueSetUri = vsd.getDefinedValueSet().getUri();

		if (StringUtils.isBlank(valueSetLocalName) && StringUtils.isBlank(valueSetUri))
		{
			if (StringUtils.isBlank(vsd.getDefinedValueSet().getHref()))
			{
				throw new UnspecifiedCts2Exception("The 'DefinedValueSet' must specify which ValueSet is being referenced");
			}
			else
			{
				ValueSetCatalogEntry vs = utilities.lookupValueSetByHref(vsd.getDefinedValueSet().getHref());
				valueSetLocalName = vs.getValueSetName();
				valueSetUri = vs.getAbout();
			}
		}
		else if (StringUtils.isBlank(valueSetLocalName))
		{
			logger_.debug("Looking up ValueSet by URI to get Name");
			// Use URI to lookup Name. Ignore href.
			ValueSetCatalogEntry vs = utilities.lookupValueSetByURI(valueSetUri, null);
			valueSetLocalName = vs.getValueSetName();
		}
		else if (StringUtils.isBlank(valueSetUri))
		{
			logger_.debug("Looking up ValueSet by Name to get URI");
			// Use name to lookup URI. Ignore href.
			ValueSetCatalogEntry vs = utilities.lookupValueSetByLocalName(valueSetLocalName, null);
			valueSetUri = vs.getAbout();
		}
		else
		{
			// Both populated, use as is. Ignore href
			logger_.debug("Using ValueSet Name and  URI as provided");

			try
			{
				new URI(valueSetUri);
			}
			catch (URISyntaxException e)
			{
				throw new UnspecifiedCts2Exception("The DefinedValueSet 'URI' field must be a valid URI - " + e);
			}
			
			try
			{
				ValueSetCatalogEntry vs = utilities.lookupValueSetByURI(valueSetUri, null);
				if (vs == null)
				{
					throw new UnknownValueSet();
				}
				valueSetLocalName = vs.getValueSetName();
			}
			catch (UnknownValueSet e)
			{
				//Not present... lets store it in our local catalog.
				ValueSetCatalogEntry vsce = new ValueSetCatalogEntry();
				vsce.setAbout(valueSetUri);
				vsce.setFormalName(valueSetLocalName);
				vsce = utilities.getLocalValueSetMaintenanceService().createResource(vsce);
				valueSetLocalName = vsce.getValueSetName();  //This may have been changed to make it unique, during the write.
			}
		}

		// Should have the valueSetName and valueSetURI now.

		// Create a unique localname
		String localName = vsd.getFormalName();
		if (StringUtils.isBlank(localName))
		{
			localName = String.valueOf(vsId.getAndIncrement());
		}

		// make unique
		while (altNameHashes_.containsKey(localName + ":" + valueSetLocalName) || altNameHashes_.containsKey(localName + ":" + valueSetUri))
		{
			localName += "-" + vsId.getAndIncrement();
		}

		valueSets_.put(uri, new ValueSetDefinitionWithLocalName(vsd, localName, valueSetLocalName, valueSetUri));
		vsd.getDefinedValueSet().setContent(valueSetLocalName);  //make sure what we store actually matches what the service we found it on is using
		altNameHashes_.put(localName + ":" + valueSetLocalName, uri);
		altNameHashes_.put(localName + ":" + valueSetUri, uri);

		return new LocalIdValueSetDefinition(localName, vsd);
	}

	protected void delete(ValueSetDefinitionReadId id)
	{
		ValueSetDefinitionWithLocalName removed = null;
		if (StringUtils.isNotBlank(id.getUri()))
		{
			removed = valueSets_.remove(id.getUri());
		}
		else if (StringUtils.isNotBlank(id.getName()))
		{
			String uri = null;
			if (id.getValueSet() == null)
			{
				throw ExceptionBuilder.buildUnknownValueSetReference("A URI or Name is required for the ValueSet when the ValueSetDefinitionURI is not provided");
			}
			else if (StringUtils.isNotBlank(id.getValueSet().getName()))
			{
				uri = altNameHashes_.get(id.getName() + ":" + id.getValueSet().getName());
			}
			else if (StringUtils.isNotBlank(id.getValueSet().getUri()))
			{
				uri = altNameHashes_.get(id.getName() + ":" + id.getValueSet().getUri());
			}
			else
			{
				throw new UnspecifiedCts2Exception("A URI or Name is required for the ValueSet when the ValueSetDefinitionURI is not provided");
			}

			if (uri == null)
			{
				throw ExceptionBuilder.buildUnknownValueSetReference("No URI found");
			}
			else
			{
				removed = valueSets_.get(uri);
			}
		}
		else
		{
			throw new UnspecifiedCts2Exception("A URI or Name is required for the ValueSetDefinition");
		}

		if (removed == null)
		{
			throw ExceptionBuilder.buildUnknownValueSetReference("Remove failed - ValueSet was not present");
		}

		// Clean up the altNameHashes
		if (null == altNameHashes_.remove(removed.localName_ + ":" + removed.valueSetLocalName_))
		{
			logger_.error("Internal failure on delete - mapping miss!", new Exception());
			throw new RuntimeException("Coding Error!");
		}

		if (null == altNameHashes_.remove(removed.localName_ + ":" + removed.valueSetURI_))
		{
			logger_.error("Internal failure on delete - mapping miss!", new Exception());
			throw new RuntimeException("Coding Error!");
		}
	}

	private class ValueSetDefinitionWithLocalName
	{
		private ValueSetDefinition vsd_;
		private String localName_;
		private String valueSetLocalName_;
		private String valueSetURI_;

		protected ValueSetDefinitionWithLocalName(ValueSetDefinition vsd, String localName, String valueSetLocalName, String valueSetURI)
		{
			this.vsd_ = vsd;
			this.localName_ = localName;
			this.valueSetLocalName_ = valueSetLocalName;
			this.valueSetURI_ = valueSetURI;
		}
	}
}
