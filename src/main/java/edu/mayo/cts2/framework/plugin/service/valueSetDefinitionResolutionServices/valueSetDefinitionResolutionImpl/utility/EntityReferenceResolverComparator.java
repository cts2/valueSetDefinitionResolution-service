package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.valueSetDefinitionResolutionImpl.utility;

import java.util.Comparator;
import edu.mayo.cts2.framework.model.core.SortCriteria;
import edu.mayo.cts2.framework.model.core.SortCriterion;
import edu.mayo.cts2.framework.model.core.types.SortDirection;
import edu.mayo.cts2.framework.model.exception.UnspecifiedCts2Exception;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ctsUtility.AlphanumComparator;
import edu.mayo.cts2.framework.service.meta.StandardModelAttributeReference;

public class EntityReferenceResolverComparator implements Comparator<EntityReferenceResolver>
{
	private SortCriteria sc_;

	public EntityReferenceResolverComparator(SortCriteria sc)
	{
		sc_ = sc;
	}

	@Override
	public int compare(EntityReferenceResolver o1, EntityReferenceResolver o2)
	{
		if (o1 == null)
		{
			return -1;
		}
		if (o2 == null)
		{
			return 1;
		}
		for (SortCriterion sort : sc_.getEntryAsReference())
		{
			int result = AlphanumComparator.compare(getComparable(o1, sort.getSortElement().getAttributeReference()), 
					getComparable(o2, sort.getSortElement().getAttributeReference()), true) * (sort.getSortDirection() == SortDirection.DESCENDING ? -1 : 1);
			if (result < 0 || result > 0)
			{
				return result;
			}
		}
		return 0;
	}
	
	private String getComparable(EntityReferenceResolver err, String sortElement)
	{
		if (StandardModelAttributeReference.DESIGNATION.getModelAttributeReference().getContent().equals(sortElement))
		{
			return err.getEntity().getDesignation();
		}
		else if (StandardModelAttributeReference.RESOURCE_NAME.getModelAttributeReference().getContent().equals(sortElement))
		{
			return err.getEntity().getName();
		}
		else
		{
			throw new UnspecifiedCts2Exception("That shouldn't have happened...");
		}
	}
}
