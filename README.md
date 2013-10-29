#### valueSetDefinitionResolution-service

An Implementation of the ValueSetDefinitionResolution Service


##### A CTS2 ValueSetDefinitionResolutionService implemented as a plugin for the CTS2 Development Framework.


What It Includes


* A ServiceProvider implementation
* A ValueSetDefinitionResolution Service implementation

It also includes:
* Transient, basic implementations of the ValueSetDefinition Maintenance/Query/Read Services
* Transient, basic implementations of the ValueSet Maintenance/Query/Read Services.

These might be useful with a stand-alone deployment... but are not used when this library is added into the exist-service implementation, for example.

For debugging purposes, you may find it useful to edit this file: cts2-framework/webapp/src/main/resources/log4j.xml
and add this:

	<logger name="edu.mayo.cts2.framework.plugin.service.valueSetDefinitionResolutionServices">
		<level value="debug" />
	</logger>

This code supports running in one of 2 main modes.  

* In the first mode, when this code is embedded into another server like the exist-service - everything 
(with the exception of Directory Query URI resolution) runs directly against the local services.  The 
configuration file valueSetDefinition_config_blank.properties should be used to keep spring happy - it 
is simply blank.  Note, however - you can specify another external server in the config file if you like - 
if the local service is unable to resolve something, the remote service will be tried.

* In the second mode - everything is resolved via a REST call to a remote server, where the address is 
specified in the valueSetDefinition_config.properties file.