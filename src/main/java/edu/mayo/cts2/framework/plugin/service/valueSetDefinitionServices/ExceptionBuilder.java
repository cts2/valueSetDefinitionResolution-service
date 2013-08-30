package edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices;

import edu.mayo.cts2.framework.model.core.OpaqueData;
import edu.mayo.cts2.framework.model.core.TsAnyType;
import edu.mayo.cts2.framework.model.service.exception.CTS2Exception;
import edu.mayo.cts2.framework.model.service.exception.UnknownCodeSystemVersion;
import edu.mayo.cts2.framework.model.service.exception.UnknownEntity;

public class ExceptionBuilder
{
	public static UnknownCodeSystemVersion buildUnknownCodeSystemVersion(String message)
	{
		UnknownCodeSystemVersion e = new UnknownCodeSystemVersion();
		addMessage(message, e);
		return e;
	}
	
	public static UnknownEntity buildUnknownEntity(String message)
	{
		UnknownEntity e = new UnknownEntity();
		addMessage(message, e);
		return e;
	}
	
	private static void addMessage(String message, CTS2Exception e)
	{
		OpaqueData cts2Message = new OpaqueData();
		TsAnyType any = new TsAnyType();
		any.setContent(message);
		cts2Message.setValue(any);
		e.setCts2Message(cts2Message);
	}
}