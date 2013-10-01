#### valueSetDefinitionResolution-service

An Implementation of the ValueSetDefinitionResolution Service


##### A CTS2 ValueSetDefinitionResolutionService implemented as a plugin for the CTS2 Development Framework.


What It Includes


* A ServiceProvider implementation
* A ValueSetDefinitionResolution Service implementation

It also includes:
* Transient, basic implementations of the ValueSetDefinition Maintenance/Query/Read Services
* Transient basic implementations of the ValueSet Maintenance/Query/Read Services.

These might be useful with a stand-alone deployment... but are not used when this library is added into the exist-service implementation, for example.


For debugging purposes, you may find it useful to edit this file: cts2-framework/webapp/src/main/resources/log4j.xml
and add this:

	<logger name="edu.mayo.cts2.framework.plugin.service.valueSetDefinitionServices">
		<level value="debug" />
	</logger>
