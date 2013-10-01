package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.valueSetDefinitionResolutionImpl;

import edu.mayo.cts2.framework.model.core.ComponentReference;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ctsUtility.AlphanumComparator;

public enum SupportedSorts
{
	ALPHABETIC("alphabetic"), ALPHA_NUMERIC("alphanumeric");
	
	private String niceName;
	
	SupportedSorts(String value)
	{
		this.niceName = value;
	}
	
	public String getNiceName()
	{
		return niceName;
	}
	
	public ComponentReference asComponentReference()
	{
		ComponentReference cr = new ComponentReference();
		cr.setSpecialReference(getNiceName());
		return cr;
	}
	
	public int compare(String a, String b)
	{
		if (a == null)
		{
			return -1;
		}
		if (b == null)
		{
			return 1;
		}
		if (this == ALPHABETIC)
		{
			return a.compareToIgnoreCase(b);
		}
		else if (this == ALPHA_NUMERIC)
		{
			return AlphanumComparator.compare(a, b, true);
		}
		else
		{
			throw new RuntimeException("Implementation error");
		}
	}
}
