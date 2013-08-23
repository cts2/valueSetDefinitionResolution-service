package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.valueSetDefinitionStorageStubs;

import javax.annotation.Resource;
import org.springframework.stereotype.Component;
import edu.mayo.cts2.framework.model.extension.LocalIdValueSetDefinition;
import edu.mayo.cts2.framework.model.valuesetdefinition.ValueSetDefinition;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.Utilities;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.ValueSetDefinitionSharedServiceBase;
import edu.mayo.cts2.framework.service.profile.UpdateChangeableMetadataRequest;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ValueSetDefinitionMaintenanceService;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.name.ValueSetDefinitionReadId;

@Component("valueSetDefinitionMaintenanceServiceImpl")
public class ValueSetDefinitionMaintenanceServiceImpl extends ValueSetDefinitionSharedServiceBase implements ValueSetDefinitionMaintenanceService
{
	@Resource
	private Utilities utilities_;
	
	@Override
	public void updateChangeableMetadata(ValueSetDefinitionReadId identifier, UpdateChangeableMetadataRequest request)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateResource(LocalIdValueSetDefinition resource)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public LocalIdValueSetDefinition createResource(ValueSetDefinition resource)
	{
		logger_.debug("createResource {}", resource);
		return ValueSetDefinitionStorage.getInstance().store(resource, utilities_);
	}

	@Override
	public void deleteResource(ValueSetDefinitionReadId identifier, String changeSetUri)
	{
		logger_.debug("deleteResource {}, {}", identifier, changeSetUri);
		ValueSetDefinitionStorage.getInstance().delete(identifier);
	}

	@Override
	public String getServiceName()
	{
		return "Non-Persistent " + super.getServiceName();
	}
}
