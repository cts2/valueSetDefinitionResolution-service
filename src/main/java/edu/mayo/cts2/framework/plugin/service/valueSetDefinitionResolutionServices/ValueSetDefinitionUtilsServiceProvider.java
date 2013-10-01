package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices;

import javax.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import edu.mayo.cts2.framework.service.profile.Cts2Profile;
import edu.mayo.cts2.framework.service.profile.valueset.ValueSetMaintenanceService;
import edu.mayo.cts2.framework.service.profile.valueset.ValueSetQueryService;
import edu.mayo.cts2.framework.service.profile.valueset.ValueSetReadService;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ValueSetDefinitionMaintenanceService;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ValueSetDefinitionQueryService;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ValueSetDefinitionReadService;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ValueSetDefinitionResolutionService;
import edu.mayo.cts2.framework.service.provider.ServiceProvider;

@Component("valueSetDefinitionUtilsServiceProvider")
public class ValueSetDefinitionUtilsServiceProvider implements ServiceProvider
{
	@Resource 
	private ValueSetDefinitionMaintenanceService vsdMaintS;
	
	@Resource 
	private ValueSetDefinitionQueryService vsdQueryS;
	
	@Resource 
	private ValueSetDefinitionReadService vsdReadS;
	
	@Resource 
	private ValueSetMaintenanceService vsMaintS;
	
	@Resource 
	private ValueSetQueryService vsQueryS;
	
	@Resource 
	private ValueSetReadService vsReadS;
	
	@Resource 
	private ValueSetDefinitionResolutionService vsdResolveS;

	private final Logger logger_ = LoggerFactory.getLogger(this.getClass());

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Cts2Profile> T getService(Class<T> clazz)
	{
		logger_.debug("Scan request for {}", clazz.getSimpleName());
		if (clazz == ValueSetDefinitionMaintenanceService.class)
		{
			return (T) this.vsdMaintS;
		}
		else if (clazz == ValueSetDefinitionQueryService.class)
		{
			return (T) this.vsdQueryS;
		}
		else if (clazz == ValueSetDefinitionReadService.class)
		{
			return (T) this.vsdReadS;
		}
		if (clazz == ValueSetMaintenanceService.class)
		{
			return (T) this.vsMaintS;
		}
		else if (clazz == ValueSetQueryService.class)
		{
			return (T) this.vsQueryS;
		}
		else if (clazz == ValueSetReadService.class)
		{
			return (T) this.vsReadS;
		}
		if (clazz == ValueSetDefinitionResolutionService.class)
		{
			return (T) this.vsdResolveS;
		}
		else
		{
			return null;
		}
	}
}
