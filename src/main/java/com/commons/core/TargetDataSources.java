package com.commons.core;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public class TargetDataSources {
	
	private static Map<Object,Object> targetDataSources = new HashMap<>();

	public static Map<Object, Object> getTargetDataSources() {
		return targetDataSources;
	}

	public static void setTargetDataSources(Map<Object, Object> targetDataSources) {
		TargetDataSources.targetDataSources = targetDataSources;
	}
	
	
	
	
}
