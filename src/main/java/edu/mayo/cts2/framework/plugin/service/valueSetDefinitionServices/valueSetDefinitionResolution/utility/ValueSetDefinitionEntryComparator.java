package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.valueSetDefinitionResolution.utility;

import java.util.Comparator;
import edu.mayo.cts2.framework.model.valuesetdefinition.ValueSetDefinitionEntry;

public class ValueSetDefinitionEntryComparator implements Comparator<ValueSetDefinitionEntry>
{
	@Override
	public int compare(ValueSetDefinitionEntry o1, ValueSetDefinitionEntry o2)
	{
		if (o1 == null)
		{
			return -1;
		}
		else if (o2 == null)
		{
			return 1;
		}
		else
		{
			if (o1.getEntryOrder() == null)
			{
				return -1;
			}
			else if (o2.getEntryOrder() == null)
			{
				return 1;
			}
			else
			{
				if (o1.getEntryOrder() < o2.getEntryOrder())
				{
					return -1;
				}
				else if (o1.getEntryOrder() > o2.getEntryOrder())
				{
					return 1;
				}
				return 0;
			}
		}
	}
}
