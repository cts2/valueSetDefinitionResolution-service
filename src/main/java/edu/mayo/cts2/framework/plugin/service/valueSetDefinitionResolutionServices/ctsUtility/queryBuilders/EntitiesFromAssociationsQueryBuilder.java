package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ctsUtility.queryBuilders;

import edu.mayo.cts2.framework.model.command.ResolvedReadContext;
import edu.mayo.cts2.framework.service.profile.association.AssociationQuery;
import edu.mayo.cts2.framework.service.profile.entitydescription.EntitiesFromAssociationsQuery;

public class EntitiesFromAssociationsQueryBuilder
{
	public static EntitiesFromAssociationsQuery build(final EntitiesFromAssociationsQuery.EntitiesFromAssociations entitiesFromAssociations,
			final ResolvedReadContext readContext)
	{
		return new EntitiesFromAssociationsQuery()
		{
			private EntitiesFromAssociations entitiesFromAssociations_ = entitiesFromAssociations;
			private final AssociationQuery associationQuery_ = AssociationQueryBuilder.build(readContext);

			@Override
			public EntitiesFromAssociations getEntitiesFromAssociationsType()
			{
				return entitiesFromAssociations_;
			}

			@Override
			public AssociationQuery getAssociationQuery()
			{
				return associationQuery_;
			}
		};
	}
}
