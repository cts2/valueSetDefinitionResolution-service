package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.valueSetDefinitionResolutionImpl.utility;

import java.util.List;
import edu.mayo.cts2.framework.model.valuesetdefinition.ResolvedValueSetHeader;

/**
 * A container for a result and the associated Header, so that is can be stored in a table for reuse if another page of the
 * same result is requested
 * 
 * @author darmbrust
 * 
 */
public class ResultCache
{
	private List<EntityReferenceResolver> items_;
	private ResolvedValueSetHeader header_;

	public ResultCache(List<EntityReferenceResolver> items, ResolvedValueSetHeader header)
	{
		items_ = items;
		header_ = header;
	}

	public List<EntityReferenceResolver> getItems()
	{
		return items_;
	}

	public ResolvedValueSetHeader getHeader()
	{
		return header_;
	}
}
