package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ctsUtility.queryBuilders;

import java.util.HashSet;
import java.util.Set;
import edu.mayo.cts2.framework.model.command.ResolvedFilter;
import edu.mayo.cts2.framework.model.command.ResolvedReadContext;
import edu.mayo.cts2.framework.model.service.core.Query;
import edu.mayo.cts2.framework.service.command.restriction.AssociationQueryServiceRestrictions;
import edu.mayo.cts2.framework.service.profile.association.AssociationQuery;

public class AssociationQueryBuilder
{
	public static AssociationQuery build(final ResolvedReadContext readContext)
	{
		return new AssociationQuery()
		{
			private final ResolvedReadContext resolvedReadContext = readContext == null ? new ResolvedReadContext() : readContext;
			private final Query query = new Query();
			private final Set<ResolvedFilter> resolvedFilter = new HashSet<ResolvedFilter>();
			private final AssociationQueryServiceRestrictions associationQueryServiceRestrictions = new AssociationQueryServiceRestrictions();

			@Override
			public ResolvedReadContext getReadContext()
			{
				return resolvedReadContext;
			}

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
			public AssociationQueryServiceRestrictions getRestrictions()
			{
				return associationQueryServiceRestrictions;
			}
		};
	}
}
