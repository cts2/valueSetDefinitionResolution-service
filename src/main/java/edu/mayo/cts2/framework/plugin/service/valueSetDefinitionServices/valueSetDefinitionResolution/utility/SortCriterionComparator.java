package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.valueSetDefinitionResolution.utility;

import java.util.Comparator;
import edu.mayo.cts2.framework.model.core.SortCriterion;

public class SortCriterionComparator implements Comparator<SortCriterion>
{
	@Override
	public int compare(SortCriterion o1, SortCriterion o2)
	{
		if (o1 == null)
		{
			return -1;
		}
		if (o2 == null)
		{
			return 1;
		}
		else
		{
			return o1.getEntryOrder().compareTo(o2.getEntryOrder());
		}
	}
}
