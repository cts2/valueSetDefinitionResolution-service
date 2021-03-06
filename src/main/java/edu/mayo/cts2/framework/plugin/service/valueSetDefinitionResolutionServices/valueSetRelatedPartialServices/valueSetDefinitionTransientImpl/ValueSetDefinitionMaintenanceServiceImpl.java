package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.valueSetRelatedPartialServices.valueSetDefinitionTransientImpl;

import javax.annotation.Resource;
import org.springframework.stereotype.Component;
import edu.mayo.cts2.framework.model.exception.UnspecifiedCts2Exception;
import edu.mayo.cts2.framework.model.extension.LocalIdValueSetDefinition;
import edu.mayo.cts2.framework.model.valuesetdefinition.ValueSetDefinition;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ValueSetDefinitionSharedServiceBase;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ctsUtility.Utilities;
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
		throw new UnspecifiedCts2Exception("Unsupported operation");
	}

	@Override
	public void updateResource(LocalIdValueSetDefinition resource)
	{
		throw new UnspecifiedCts2Exception("Unsupported operation");
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
