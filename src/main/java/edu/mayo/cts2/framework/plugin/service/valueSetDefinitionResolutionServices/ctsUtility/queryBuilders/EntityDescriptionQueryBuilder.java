package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ctsUtility.queryBuilders;

import java.util.HashSet;
import java.util.Set;
import edu.mayo.cts2.framework.model.command.ResolvedFilter;
import edu.mayo.cts2.framework.model.command.ResolvedReadContext;
import edu.mayo.cts2.framework.model.service.core.Query;
import edu.mayo.cts2.framework.service.command.restriction.EntityDescriptionQueryServiceRestrictions;
import edu.mayo.cts2.framework.service.profile.entitydescription.EntitiesFromAssociationsQuery;
import edu.mayo.cts2.framework.service.profile.entitydescription.EntityDescriptionQuery;

public class EntityDescriptionQueryBuilder
{
	public static EntityDescriptionQuery build(final ResolvedReadContext readContext)
	{
		return new EntityDescriptionQuery()
		{
			private final Query query = new Query();
			private final Set<ResolvedFilter> resolvedFilter = new HashSet<ResolvedFilter>();
			private final ResolvedReadContext resolvedReadContext = readContext == null ? new ResolvedReadContext() : readContext;
			private final EntityDescriptionQueryServiceRestrictions entityDescriptionQueryServiceRestrictions = new EntityDescriptionQueryServiceRestrictions();
			private final EntitiesFromAssociationsQuery entitiesFromAssociationQuery = EntitiesFromAssociationsQueryBuilder.build(null, resolvedReadContext);

			@Override
			public Query getQuery()
			{
				return query;
			}

			@Override
			public Set<ResolvedFilter> getFilterComponent()
			{
				return resolvedFilter;
			}

			@Override
			public ResolvedReadContext getReadContext()
			{
				return resolvedReadContext;
			}

			@Override
			public EntityDescriptionQueryServiceRestrictions getRestrictions()
			{
				return entityDescriptionQueryServiceRestrictions;
			}

			@Override
			public EntitiesFromAssociationsQuery getEntitiesFromAssociationsQuery()
			{
				return entitiesFromAssociationQuery;
			}
		};
	}
}
