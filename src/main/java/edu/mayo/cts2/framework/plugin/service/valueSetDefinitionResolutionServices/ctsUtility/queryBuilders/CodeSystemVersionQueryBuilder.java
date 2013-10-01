package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices.ctsUtility.queryBuilders;

import java.util.HashSet;
import java.util.Set;
import edu.mayo.cts2.framework.model.command.ResolvedFilter;
import edu.mayo.cts2.framework.model.command.ResolvedReadContext;
import edu.mayo.cts2.framework.model.service.core.Query;
import edu.mayo.cts2.framework.service.command.restriction.CodeSystemVersionQueryServiceRestrictions;
import edu.mayo.cts2.framework.service.profile.codesystemversion.CodeSystemVersionQuery;

public class CodeSystemVersionQueryBuilder
{
	public static CodeSystemVersionQuery build(final ResolvedReadContext readContext)
	{
		return new CodeSystemVersionQuery()
		{
			private final Query query = new Query();
			private final Set<ResolvedFilter> resolvedFilter = new HashSet<ResolvedFilter>();
			private final ResolvedReadContext resolvedReadContext = readContext == null ? new ResolvedReadContext() : readContext;
			private final CodeSystemVersionQueryServiceRestrictions codeSystemVersionQueryServiceRestrictions = new CodeSystemVersionQueryServiceRestrictions();

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
			public CodeSystemVersionQueryServiceRestrictions getRestrictions()
			{
				return codeSystemVersionQueryServiceRestrictions;
			}
		};
	}
}
