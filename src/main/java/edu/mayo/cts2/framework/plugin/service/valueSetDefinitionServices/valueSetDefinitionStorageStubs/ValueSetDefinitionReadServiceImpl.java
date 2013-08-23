package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.valueSetDefinitionStorageStubs;

import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Component;
import edu.mayo.cts2.framework.model.command.ResolvedReadContext;
import edu.mayo.cts2.framework.model.core.VersionTagReference;
import edu.mayo.cts2.framework.model.extension.LocalIdValueSetDefinition;
import edu.mayo.cts2.framework.model.service.core.NameOrURI;
import edu.mayo.cts2.framework.model.service.exception.UnknownValueSetDefinition;
import edu.mayo.cts2.framework.model.service.exception.UnsupportedVersionTag;
import edu.mayo.cts2.framework.model.valuesetdefinition.ValueSetDefinition;
import edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.ValueSetDefinitionSharedServiceBase;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ValueSetDefinitionReadService;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.name.ValueSetDefinitionReadId;

@Component("valueSetDefinitionReadServiceImpl")
public class ValueSetDefinitionReadServiceImpl extends ValueSetDefinitionSharedServiceBase implements ValueSetDefinitionReadService
{
	@Override
	public LocalIdValueSetDefinition readByTag(NameOrURI parentIdentifier, VersionTagReference tag, ResolvedReadContext readContext)
	{
		throw new UnsupportedVersionTag();
	}

	@Override
	public boolean existsByTag(NameOrURI parentIdentifier, VersionTagReference tag, ResolvedReadContext readContext)
	{
		throw new UnsupportedVersionTag();
	}

	@Override
	public List<VersionTagReference> getSupportedTags()
	{
		return new ArrayList<VersionTagReference>();
	}

	@Override
	public LocalIdValueSetDefinition read(ValueSetDefinitionReadId identifier, ResolvedReadContext readContext)
	{
		logger_.debug("read {}, {}", identifier, readContext);
		// Note - this implementation ignores readContext
		ValueSetDefinition vsd = ValueSetDefinitionStorage.getInstance().get(identifier);
		if (vsd == null)
		{
			throw new UnknownValueSetDefinition();
		}
		return new LocalIdValueSetDefinition(ValueSetDefinitionStorage.getInstance().getLocalName(vsd), vsd);
	}

	@Override
	public boolean exists(ValueSetDefinitionReadId identifier, ResolvedReadContext readContext)
	{
		logger_.debug("exists {}, {}", identifier, readContext);
		// Note - this implementation ignores readContext
		return ValueSetDefinitionStorage.getInstance().get(identifier) != null;
	}

	@Override
	public String getServiceName()
	{
		return "Non-Persistent " + super.getServiceName();
	}
}
