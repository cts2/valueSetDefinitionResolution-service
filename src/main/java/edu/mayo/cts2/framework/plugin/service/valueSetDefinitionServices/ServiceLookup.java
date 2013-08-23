package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices;

import javax.annotation.Resource;
import edu.mayo.cts2.framework.service.profile.association.AssociationQueryService;
import edu.mayo.cts2.framework.service.profile.codesystemversion.CodeSystemVersionQueryService;
import edu.mayo.cts2.framework.service.profile.codesystemversion.CodeSystemVersionReadService;
import edu.mayo.cts2.framework.service.profile.entitydescription.EntityDescriptionQueryService;
import edu.mayo.cts2.framework.service.profile.entitydescription.EntityDescriptionReadService;
import edu.mayo.cts2.framework.service.profile.valueset.ValueSetMaintenanceService;
import edu.mayo.cts2.framework.service.profile.valueset.ValueSetReadService;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ValueSetDefinitionQueryService;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ValueSetDefinitionReadService;
import edu.mayo.cts2.framework.service.provider.ServiceProvider;

public class ServiceLookup
{
	@Resource(name="valueSetDefinitionUtilsServiceProvider")
	private static ServiceProvider localValueSetDefinitionUtilsServiceProvider_;
	
	//TODO FRAMEWORK - We need a way to have multiple service providers - https://github.com/cts2/cts2-framework/issues/29
	//within a single instance - as the service provider that I am providing above isn't going to return hits for the unimplemented methods below.
	
	public static ValueSetDefinitionQueryService getLocalValueSetDefinitionQueryService()
	{
		return localValueSetDefinitionUtilsServiceProvider_.getService(ValueSetDefinitionQueryService.class);
	}
	
	public static ValueSetReadService getLocalValueSetReadService()
	{
		return localValueSetDefinitionUtilsServiceProvider_.getService(ValueSetReadService.class);
	}
	
	public static ValueSetMaintenanceService getLocalValueSetMaintenanceService()
	{
		return localValueSetDefinitionUtilsServiceProvider_.getService(ValueSetMaintenanceService.class);
	}
	
	public static ValueSetDefinitionReadService getLocalValueSetDefinitionReadService()
	{
		return localValueSetDefinitionUtilsServiceProvider_.getService(ValueSetDefinitionReadService.class);
	}
	
	public static EntityDescriptionReadService getLocalEntityDescriptionReadService()
	{
		return null;
	}

	public static EntityDescriptionQueryService getLocalEntityDescriptionQueryService()
	{
		return null;
	}

	public static CodeSystemVersionReadService getLocalCodeSystemVersionReadService()
	{
		return null;
	}

	public static CodeSystemVersionQueryService getLocalCodeSystemVersionQueryService()
	{
		return null;
	}

	public static AssociationQueryService getLocalAssociationQueryService()
	{
		return null;
	}
}
