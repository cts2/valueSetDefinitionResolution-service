package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.valueSetStorageStubs;

import org.springframework.stereotype.Component;
import edu.mayo.cts2.framework.model.exception.UnspecifiedCts2Exception;
import edu.mayo.cts2.framework.model.service.core.NameOrURI;
import edu.mayo.cts2.framework.model.valueset.ValueSetCatalogEntry;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.ValueSetDefinitionSharedServiceBase;
import edu.mayo.cts2.framework.service.profile.UpdateChangeableMetadataRequest;
import edu.mayo.cts2.framework.service.profile.valueset.ValueSetMaintenanceService;

@Component("valueSetMaintenanceServiceImpl")
public class ValueSetMaintenanceServiceImpl extends ValueSetDefinitionSharedServiceBase implements ValueSetMaintenanceService
{
	@Override
	public void updateChangeableMetadata(NameOrURI identifier, UpdateChangeableMetadataRequest request)
	{
		throw new UnspecifiedCts2Exception("Unsupported operation");
	}

	@Override
	public void updateResource(ValueSetCatalogEntry resource)
	{
		throw new UnspecifiedCts2Exception("Unsupported Operation");
	}

	@Override
	public ValueSetCatalogEntry createResource(ValueSetCatalogEntry resource)
	{
		logger_.debug("createResource {}", resource);
		return ValueSetStorage.getInstance().store(resource);
	}

	@Override
	public void deleteResource(NameOrURI identifier, String changeSetUri)
	{
		logger_.debug("deleteResource {}", identifier, changeSetUri);
		// note - no support for changeSetURI
		ValueSetStorage.getInstance().delete(identifier);
	}

	@Override
	public String getServiceName()
	{
		return "Non-Persistent " + super.getServiceName();
	}
}
