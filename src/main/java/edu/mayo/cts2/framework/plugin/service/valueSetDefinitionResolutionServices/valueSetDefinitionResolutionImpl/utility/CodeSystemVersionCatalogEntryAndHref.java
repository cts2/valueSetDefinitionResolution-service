package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.valueSetDefinitionResolutionImpl.utility;

import edu.mayo.cts2.framework.model.codesystemversion.CodeSystemVersionCatalogEntry;
import edu.mayo.cts2.framework.model.codesystemversion.CodeSystemVersionCatalogEntryMsg;
import edu.mayo.cts2.framework.model.core.CodeSystemVersionReference;
import edu.mayo.cts2.framework.model.core.NameAndMeaningReference;

/**
 * Wrapper class to let me track the Href that goes along with a CodeSystemVersionCatalogEntry.
 * 
 * @author darmbrust
 * 
 */
public class CodeSystemVersionCatalogEntryAndHref
{
	private CodeSystemVersionCatalogEntry csvce_;
	private String href_;

	public CodeSystemVersionCatalogEntryAndHref(CodeSystemVersionCatalogEntry codeSystemVersionCatalogEntryAndHref, String href)
	{
		csvce_ = codeSystemVersionCatalogEntryAndHref;
		href_ = href;
	}

	public CodeSystemVersionCatalogEntryAndHref(CodeSystemVersionCatalogEntryMsg entityReference)
	{
		csvce_ = entityReference.getCodeSystemVersionCatalogEntry();
		href_ = entityReference.getHeading().getResourceRoot() + entityReference.getHeading().getResourceURI();
	}

	public CodeSystemVersionCatalogEntry getCodeSystemVersionCatalogEntry()
	{
		return csvce_;
	}

	public String getHref()
	{
		return href_;
	}
	
	public CodeSystemVersionReference getAsCodeSystemVersionReference()
	{
		CodeSystemVersionReference result = new CodeSystemVersionReference();
		result.setCodeSystem(csvce_.getVersionOf());
		NameAndMeaningReference namr = new NameAndMeaningReference();
		namr.setContent(csvce_.getCodeSystemVersionName());
		namr.setHref(href_);
		namr.setUri(csvce_.getAbout());
		result.setVersion(namr);
		return result;
	}
}
