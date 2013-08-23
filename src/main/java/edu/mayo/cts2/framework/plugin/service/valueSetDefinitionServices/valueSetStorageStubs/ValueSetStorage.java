package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.valueSetStorageStubs;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.HashBiMap;
import edu.mayo.cts2.framework.model.exception.UnspecifiedCts2Exception;
import edu.mayo.cts2.framework.model.service.core.NameOrURI;
import edu.mayo.cts2.framework.model.service.exception.DuplicateValueSetURI;
import edu.mayo.cts2.framework.model.service.exception.UnknownValueSet;
import edu.mayo.cts2.framework.model.valueset.ValueSetCatalogEntry;

public class ValueSetStorage
{
	private static volatile ValueSetStorage vsds_;
	// ID to ValueSet
	private HashMap<String, ValueSetCatalogEntry> valueSets_ = new HashMap<String, ValueSetCatalogEntry>();
	// Name to ID (and back again)
	private HashBiMap<String, String> nameToId_ = HashBiMap.create();
	private AtomicInteger uniqueId_ = new AtomicInteger(0);
	private final Logger logger_ = LoggerFactory.getLogger(this.getClass());

	public static ValueSetStorage getInstance()
	{
		if (vsds_ == null)
		{
			synchronized (ValueSetStorage.class)
			{
				if (vsds_ == null)
				{
					vsds_ = new ValueSetStorage();
				}
			}
		}
		return vsds_;
	}

	private ValueSetStorage()
	{

	}

	protected boolean exists(NameOrURI identifier)
	{
		String name = identifier.getName();
		String id = identifier.getUri();

		if (StringUtils.isBlank(name) && StringUtils.isBlank(id))
		{
			return false;
		}

		if (StringUtils.isBlank(id))
		{
			return valueSets_.containsKey(nameToId_.get(name));
		}
		else
		{
			return valueSets_.containsKey(id);
		}
	}

	protected ValueSetCatalogEntry get(NameOrURI identifier)
	{
		String name = identifier.getName();
		String id = identifier.getUri();

		if (StringUtils.isBlank(name) && StringUtils.isBlank(id))
		{
			throw new IllegalArgumentException("Either the name or ID must be specified");
		}

		if (StringUtils.isBlank(id))
		{
			return valueSets_.get(nameToId_.get(name));
		}
		else
		{
			return valueSets_.get(id);
		}
	}

	protected Collection<ValueSetCatalogEntry> getAll()
	{
		return valueSets_.values();
	}

	protected ValueSetCatalogEntry store(ValueSetCatalogEntry vsce)
	{
		String name = vsce.getValueSetName();

		// Use the name the provided, if we can. But we have to make it unique....
		if (StringUtils.isBlank(name))
		{
			// build one from formal name
			if (StringUtils.isBlank(vsce.getFormalName()))
			{
				name = String.valueOf(uniqueId_.getAndIncrement());
			}
			else
			{
				name = vsce.getFormalName();
			}
		}

		while (nameToId_.containsKey(name))
		{
			name += "-" + uniqueId_.getAndIncrement();
		}
		vsce.setValueSetName(name);

		String id = vsce.getAbout();
		if (StringUtils.isBlank(id))
		{
			throw new UnspecifiedCts2Exception("The ValueSet 'About' field must be populated");
		}
		if (valueSets_.containsKey(id))
		{
			throw new DuplicateValueSetURI();
		}

		try
		{
			new URI(id);
		}
		catch (URISyntaxException e)
		{
			throw new UnspecifiedCts2Exception("The ValueSet 'About' field must be a valid URI - " + e);
		}

		valueSets_.put(id, vsce);
		nameToId_.put(name, id);

		return vsce;
	}

	protected void delete(NameOrURI identifier)
	{

		String name = identifier.getName();
		String id = identifier.getUri();

		if (StringUtils.isBlank(name) && StringUtils.isBlank(id))
		{
			throw new UnknownValueSet();
		}

		if (StringUtils.isBlank(id))
		{
			id = nameToId_.remove(name);
			if (id == null)
			{
				throw new UnknownValueSet();
			}
			ValueSetCatalogEntry vs = valueSets_.remove(id);
			if (vs == null)
			{
				logger_.error("Coding Error - ValueSet missing?", new Exception());
				throw new RuntimeException("Internal Error");
			}
		}
		else
		{
			ValueSetCatalogEntry vs = valueSets_.remove(id);
			if (vs == null)
			{
				throw new UnknownValueSet();
			}
			else
			{
				nameToId_.inverse().remove(id);
			}
		}
	}
}
