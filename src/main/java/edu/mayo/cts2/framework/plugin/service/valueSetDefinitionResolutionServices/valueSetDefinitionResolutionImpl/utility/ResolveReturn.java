package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.valueSetDefinitionResolutionImpl.utility;

import java.util.ArrayList;
import edu.mayo.cts2.framework.model.command.Page;
import edu.mayo.cts2.framework.model.core.URIAndEntityName;
import edu.mayo.cts2.framework.model.entity.EntityDirectoryEntry;
import edu.mayo.cts2.framework.model.valuesetdefinition.ResolvedValueSetHeader;

/**
 * Handles the paging, and the different types of entities that need to be returned.
 * 
 * @author darmbrust
 */
public class ResolveReturn
{
	private ResultCache resultCache_;
	private Page page_;

	public ResolveReturn(ResultCache resultCache, Page page)
	{
		this.resultCache_ = resultCache;
		this.page_ = page;
	}

	public ArrayList<URIAndEntityName> getItems()
	{
		ArrayList<URIAndEntityName> result = new ArrayList<>(page_ == null ? resultCache_.getItems().size() : page_.getMaxToReturn());
		int start = (page_ == null ? 0 : page_.getStart());
		int end = (page_ == null ? resultCache_.getItems().size() : (page_.getEnd() > resultCache_.getItems().size() ? resultCache_.getItems().size() : page_.getEnd()));
		for (int i = start; i < end; i++)
		{
			result.add(resultCache_.getItems().get(i).getEntity());
		}
		return result;
	}

	public ArrayList<EntityDirectoryEntry> getEntityDirectoryEntry()
	{
		ArrayList<EntityDirectoryEntry> result = new ArrayList<>(page_ == null ? resultCache_.getItems().size() : page_.getMaxToReturn());
		int start = (page_ == null ? 0 : page_.getStart());
		int end = (page_ == null ? resultCache_.getItems().size() : (page_.getEnd() > resultCache_.getItems().size() ? resultCache_.getItems().size() : page_.getEnd()));
		for (int i = start; i < end; i++)
		{
			result.add(resultCache_.getItems().get(i).buildEntityDirectoryEntity());
		}
		return result;
	}

	public ResolvedValueSetHeader getHeader()
	{
		return resultCache_.getHeader();
	}

	public boolean isAtEnd()
	{
		if (page_ == null)
		{
			return true;
		}
		else
		{
			return page_.getEnd() >= resultCache_.getItems().size();
		}
	}
}