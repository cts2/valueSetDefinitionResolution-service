package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices;

import java.util.ArrayList;
import java.util.Comparator;

public class EntityReferenceResolverComparator implements Comparator<EntityReferenceResolver>
{
	private ArrayList<SupportedSorts> ss_;

	public EntityReferenceResolverComparator(ArrayList<SupportedSorts> ss)
	{
		ss_ = ss;
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
		for (SupportedSorts ss : ss_)
		{
			int result = ss.compare(o1.getEntity().getName(), o2.getEntity().getName());
			if (result < 0 || result > 0)
			{
				return result;
			}
		}
		return 0;
	}
}
