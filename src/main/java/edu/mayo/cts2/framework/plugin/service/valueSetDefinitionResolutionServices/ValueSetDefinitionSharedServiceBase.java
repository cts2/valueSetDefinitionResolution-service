package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import edu.mayo.cts2.framework.model.core.FormatReference;
import edu.mayo.cts2.framework.model.core.LanguageReference;
import edu.mayo.cts2.framework.model.core.OpaqueData;
import edu.mayo.cts2.framework.model.core.SourceReference;
import edu.mayo.cts2.framework.model.core.TsAnyType;
import edu.mayo.cts2.framework.model.service.core.DocumentedNamespaceReference;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ctsUtility.Utilities;
import edu.mayo.cts2.framework.service.profile.BaseService;

public abstract class ValueSetDefinitionSharedServiceBase implements BaseService
{
	@Value("#{valueSetDefinitionResolutionBuildProperties.buildversion}") 
	protected String buildVersion;

	@Value("#{valueSetDefinitionResolutionBuildProperties.name}") 
	protected String buildName;

	@Value("#{valueSetDefinitionResolutionBuildProperties.description}") 
	protected String buildDescription;

	@Resource 
	protected Utilities utilities_;

	protected final Logger logger_ = LoggerFactory.getLogger(this.getClass());

	@Override
	public OpaqueData getServiceDescription()
	{
		OpaqueData od = new OpaqueData();
		od.setFormat(new FormatReference("text/plain"));
		od.setLanguage(new LanguageReference("en"));
		TsAnyType any = new TsAnyType();
		any.setContent(buildDescription);
		od.setValue(any);
		return od;
	}

	@Override
	public String getServiceName()
	{
		return this.getClass().getSimpleName() + " - " + buildName;
	}

	@Override
	public List<DocumentedNamespaceReference> getKnownNamespaceList()
	{
		return new ArrayList<DocumentedNamespaceReference>();
	}

	@Override
	public SourceReference getServiceProvider()
	{
		return new SourceReference("Mayo Clinic");
	}

	@Override
	public String getServiceVersion()
	{
		return buildVersion;
	}
}
