package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.valueSetRelatedPartialServices.valueSetTransientImpl;

import org.springframework.stereotype.Component;
import edu.mayo.cts2.framework.model.command.ResolvedReadContext;
import edu.mayo.cts2.framework.model.service.core.NameOrURI;
import edu.mayo.cts2.framework.model.valueset.ValueSetCatalogEntry;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ValueSetDefinitionSharedServiceBase;
import edu.mayo.cts2.framework.service.profile.valueset.ValueSetReadService;

@Component("valueSetReadServiceImpl")
public class ValueSetReadServiceImpl extends ValueSetDefinitionSharedServiceBase implements ValueSetReadService
{
	@Override
	public String getServiceName()
	{
		return "Non-Persistent " + super.getServiceName();
	}

	@Override
	public ValueSetCatalogEntry read(NameOrURI identifier, ResolvedReadContext readContext)
	{
		logger_.debug("read {}, {}", identifier, readContext);
		ValueSetCatalogEntry vs = ValueSetStorage.getInstance().get(identifier);
		if (vs != null)
		{
			utilities_.updateValueSetForReturn(vs, utilities_.getUrlConstructor().createValueSetUrl(vs.getValueSetName()), readContext);
		}
		return vs;
	}

	@Override
	public boolean exists(NameOrURI identifier, ResolvedReadContext readContext)
	{
		logger_.debug("exists {}, {}", identifier, readContext);
		return ValueSetStorage.getInstance().exists(identifier);
	}
}
