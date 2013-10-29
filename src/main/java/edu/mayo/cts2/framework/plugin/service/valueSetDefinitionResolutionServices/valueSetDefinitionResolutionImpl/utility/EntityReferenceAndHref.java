package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.valueSetDefinitionResolutionImpl.utility;

import edu.mayo.cts2.framework.model.core.EntityReference;
import edu.mayo.cts2.framework.model.entity.EntityReferenceMsg;

/**
 * Wrapper class to let me track the Href that goes along with a resolved entity.
 * 
 * @author darmbrust
 * 
 */
public class EntityReferenceAndHref
{
	private EntityReference entityReference_;
	private String href_;

	public EntityReferenceAndHref(EntityReference entityReference, String href)
	{
		entityReference_ = entityReference;
		href_ = href;
	}

	public EntityReferenceAndHref(EntityReferenceMsg entityReference)
	{
		entityReference_ = entityReference.getEntityReference();
		String sep = "";
		if (!entityReference.getHeading().getResourceURI().endsWith("/") && !entityReference.getHeading().getResourceRoot().startsWith("/"))
		{
			sep = "/";
		}
		href_ = entityReference.getHeading().getResourceRoot() + sep + entityReference.getHeading().getResourceURI();
	}

	public EntityReference getEntityReference()
	{
		return entityReference_;
	}

	public String getHref()
	{
		return href_;
	}

	@Override
	public String toString()
	{
		return entityReference_.toString();
	}
	
	
}
