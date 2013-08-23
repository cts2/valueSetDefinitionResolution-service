package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices.queries;

import java.util.HashSet;
import java.util.Set;
import edu.mayo.cts2.framework.model.command.ResolvedFilter;
import edu.mayo.cts2.framework.model.command.ResolvedReadContext;
import edu.mayo.cts2.framework.model.service.core.Query;
import edu.mayo.cts2.framework.service.command.restriction.ValueSetDefinitionQueryServiceRestrictions;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ValueSetDefinitionQuery;

public class ValueSetDefinitionQueryBuilder
{
	public static ValueSetDefinitionQuery build(final ResolvedReadContext readContext)
	{
		return new ValueSetDefinitionQuery()
		{
			private final Query query = new Query();
			private final Set<ResolvedFilter> resolvedFilter = new HashSet<ResolvedFilter>();
			private final ValueSetDefinitionQueryServiceRestrictions restrictions = new ValueSetDefinitionQueryServiceRestrictions();

			@Override
			public ResolvedReadContext getReadContext()
			{
				return readContext;
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
			public ValueSetDefinitionQueryServiceRestrictions getRestrictions()
			{
				return restrictions;
			}
		};
	}
}
