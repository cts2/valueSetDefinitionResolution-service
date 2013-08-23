package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.valueSetDefinitionResolution.utility;

import java.util.ArrayList;
import edu.mayo.cts2.framework.model.valuesetdefinition.ResolvedValueSetHeader;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.EntityReferenceResolver;

/**
 * A container for a result and the associated Header, so that is can be stored in a table for reuse if another page of the
 * same result is requested
 * 
 * @author darmbrust
 * 
 */
public class ResultCache
{
	private ArrayList<EntityReferenceResolver> items_;
	private ResolvedValueSetHeader header_;

	public ResultCache(ArrayList<EntityReferenceResolver> items, ResolvedValueSetHeader header)
	{
		items_ = items;
		header_ = header;
	}

	public ArrayList<EntityReferenceResolver> getItems()
	{
		return items_;
	}

	public ResolvedValueSetHeader getHeader()
	{
		return header_;
	}
}
