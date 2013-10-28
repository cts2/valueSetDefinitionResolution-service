package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ctsUtility.queryBuilders;

import java.util.HashSet;
import java.util.Set;
import edu.mayo.cts2.framework.model.command.ResolvedFilter;
import edu.mayo.cts2.framework.model.service.core.Query;
import edu.mayo.cts2.framework.service.command.restriction.ResolvedValueSetResolutionEntityRestrictions;
import edu.mayo.cts2.framework.service.profile.valuesetdefinition.ResolvedValueSetResolutionEntityQuery;

public class ResolvedValueSetResolutionEntityQueryBuilder
{
	public static ResolvedValueSetResolutionEntityQuery build()
	{
		return new ResolvedValueSetResolutionEntityQuery()
		{
			private final Query query = new Query();
			private final Set<ResolvedFilter> resolvedFilter = new HashSet<ResolvedFilter>();
			private final ResolvedValueSetResolutionEntityRestrictions rvsrer = new ResolvedValueSetResolutionEntityRestrictions();
			
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
			public ResolvedValueSetResolutionEntityRestrictions getResolvedValueSetResolutionEntityRestrictions()
			{
				return rvsrer;
			}
		};
	}
}
