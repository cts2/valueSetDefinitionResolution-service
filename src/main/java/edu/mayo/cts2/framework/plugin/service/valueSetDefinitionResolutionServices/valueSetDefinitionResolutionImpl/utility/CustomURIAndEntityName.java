package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.valueSetDefinitionResolutionImpl.utility;

import edu.mayo.cts2.framework.model.core.URIAndEntityName;

/**
 * Just changing .equals and .hashcode so I can hash on just the URI for cycle detection
 * 
 * @author darmbrust
 */
public class CustomURIAndEntityName
{
	private URIAndEntityName entity;

	public CustomURIAndEntityName(URIAndEntityName source)
	{
		this.entity = source;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof CustomURIAndEntityName)
		{
			return ((CustomURIAndEntityName) obj).entity.getUri().equals(entity.getUri());
		}
		return false;
	}

	@Override
	public int hashCode()
	{
		return entity.getUri().hashCode();
	}

	public URIAndEntityName getEntity()
	{
		return entity;
	}
}
