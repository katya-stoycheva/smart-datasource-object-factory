package io.dirigible.smart.datasource.object.factory;

import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameNotFoundException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.naming.spi.ObjectFactory;
import javax.sql.DataSource;

import org.apache.catalina.util.ServerInfo;
import org.apache.naming.ResourceRef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Katya Stoycheva
 *
 */
public class SmartDatasourceObjectFactory implements ObjectFactory{
	
	public static final String ENV_PREFIX_DELIMITER_DEFAULT = "_";
	public static final String ENV_PREFIX_DELIMITER_KEY = "smartobjectfactory_delimiter";
	
	private static final String DEFAULT_TOMCAT7_OF_CLASS_NAME = "org.apache.tomcat.dbcp.dbcp.BasicDataSourceFactory";
	private static final String DEFAULT_TOMCAT8_OF_CLASS_NAME = "org.apache.tomcat.dbcp.dbcp2.BasicDataSourceFactory";

	private static Logger logger = LoggerFactory.getLogger(SmartDatasourceObjectFactory.class.getName());
	private String envPrefixDelimiter;
	private ObjectFactory tomcatObjectFactory;
	
	@Override
	public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable<?, ?> environment)
			throws Exception {
		logger.debug("getObjectInstance method entry");

		if(shouldProcessResourceRef(obj)) {
			enhanceResourceRef((ResourceRef) obj, name.toString());
		}
		
		logger.debug("getObjectInstance method exit - delegating to default tomcat OF");
		return getTomcatDefaultObjectfactory().getObjectInstance(obj, name, nameCtx, environment);
	}
	
	boolean shouldProcessResourceRef(Object obj) throws NameNotFoundException {
		 if ((obj == null) || (!(obj instanceof Reference))) {
		      throw new NameNotFoundException("Object [" + obj + "] is not instance of [" + Reference.class.getName() + "]");
		    }
		    Reference ref = (Reference)obj;
		    if (DataSource.class.getName().equals(ref.getClassName())) {
		    	logger.debug("Resource ref [ " + ref.getClassName() + "] will be processed.");
		    	return true;
		    }
		    logger.debug("Resource ref [ " + ref.getClassName() + "] is discarded.");
		    logger.debug(ref.toString());
		    return false;
	}
	
	ObjectFactory getTomcatDefaultObjectfactory() throws InstantiationException, IllegalAccessException {
		if(tomcatObjectFactory == null) {
			Class<?> factoryClass; 
			
			String tomcatVersionString = ServerInfo.getServerNumber();
			logger.debug("Tomcat version string " + tomcatVersionString);
			Pattern p = Pattern.compile("[0-9].[0-9].([0-9]+)");
			Matcher match = p.matcher(tomcatVersionString);
			
			try {
				if(match.find()) {
					int majorVersion = Integer.parseInt(match.group(0).toString().substring(0,1));
					logger.debug("Major version " + majorVersion);
					if(majorVersion <= 7) {
						factoryClass = this.getClass().getClassLoader().loadClass(DEFAULT_TOMCAT7_OF_CLASS_NAME);
					} else {
						factoryClass = this.getClass().getClassLoader().loadClass(DEFAULT_TOMCAT8_OF_CLASS_NAME);	
						logger.debug("Major version " + majorVersion);
					} 
				} else {
					logger.debug("Version not found,  trying to load " + DEFAULT_TOMCAT7_OF_CLASS_NAME);
					factoryClass = this.getClass().getClassLoader().loadClass(DEFAULT_TOMCAT7_OF_CLASS_NAME);
				}
				tomcatObjectFactory = ObjectFactory.class.cast(factoryClass.newInstance()); //is new instance ok?   		    return tomcatObjectFactory;
			} catch(ClassNotFoundException e) {
		    		throw new IllegalStateException("Unable to load tomcat default object factory");
		    }
		}
		return tomcatObjectFactory;
	}
	
	void enhanceResourceRef(ResourceRef ref, String name) {
		enhanceResourceRef(ref, name, System.getenv());
	}

	void enhanceResourceRef(ResourceRef ref, String name, Map<String, String> envVars) {
		logger.debug("Enhancing resource ref...");

		String prefix = name + getPrefixDelimiter();
		
		Set<String> allEnvVarKeys = envVars.keySet();
		logger.info(" environment variables " + allEnvVarKeys);

		for (Object key : allEnvVarKeys) {
			String keyString = (String) key;
			
			if(keyString.startsWith(prefix)) {
				logger.info("Processing key [" + keyString + "]");
				
				String refAddrKey = keyString.substring(prefix.length());
				String refAddrValue = envVars.get(keyString);
				StringRefAddr refAddr = new StringRefAddr(refAddrKey, refAddrValue);
				//TODO: check for sensitive data?
				logger.info("Adding [" + refAddrKey + " / " + refAddrValue + "]" );
				
				ref.add(refAddr);
			}
		}
		logger.debug("Resource ref enhanced");
	}
	
	String getPrefixDelimiter() {
		return getPrefixDelimiter(System.getenv());
	}
	
	String getPrefixDelimiter(Map<String, String> envVariables) {
		if(envPrefixDelimiter == null) {
			String value = envVariables.get(ENV_PREFIX_DELIMITER_KEY);
			logger.debug("Delimiter value read from env variables is [" + value + "]");
			if(value == null || value.equals("")) {
				envPrefixDelimiter = ENV_PREFIX_DELIMITER_DEFAULT;
				logger.debug("Setting default delimiter " + ENV_PREFIX_DELIMITER_DEFAULT);
			} else {
				envPrefixDelimiter = value;
				logger.debug("Setting custom delimiter " + value);
			}
		}
		return envPrefixDelimiter;
	}
}
