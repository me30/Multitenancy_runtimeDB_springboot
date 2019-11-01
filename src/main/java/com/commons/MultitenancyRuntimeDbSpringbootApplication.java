package com.commons;

import java.util.Properties;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.commons.core.TargetDataSources;
import com.commons.routing.TenantAwareRoutingSource;
import com.zaxxer.hikari.HikariDataSource;

@SpringBootApplication
@EnableTransactionManagement
public class MultitenancyRuntimeDbSpringbootApplication extends SpringBootServletInitializer {

	/*public static void main(String[] args) {
		SpringApplication.run(MultitenancyRuntimeDbSpringbootApplication.class, args);
	}*/
	public static void main(String[] args) {
		new MultitenancyRuntimeDbSpringbootApplication()
				.configure(new SpringApplicationBuilder(MultitenancyRuntimeDbSpringbootApplication.class))
				.properties(getDefaultProperties())
				.run(args);
	}
	
	@Value("${db.username}")
	private String username;
	
	@Value("${db.password}")
	private String password;
	
	@Value("${db.url.Tenant}")
	private String urlTenant;
	
	@Value("${db.driverclass}")
	private String driverclass;
	
	@Bean
	public DataSource dataSource() {
		AbstractRoutingDataSource dataSource = new TenantAwareRoutingSource();
		/*Map<Object,Object> targetDataSources = new HashMap<>();
		targetDataSources.put("TenantOne", tenantOne());
		targetDataSources.put("TenantTwo", tenantTwo());
		TargetDataSources.getTargetDataSources().put("TenantOne", tenantOne());
		TargetDataSources.getTargetDataSources().put("TenantTwo", tenantTwo());*/
		dataSource.setTargetDataSources(TargetDataSources.getTargetDataSources());
		dataSource.afterPropertiesSet();
		return dataSource;
	}

	/*public DataSource tenantOne() {
		HikariDataSource dataSource = new HikariDataSource();
		dataSource.setInitializationFailTimeout(0);
		dataSource.setMaximumPoolSize(5);
		dataSource.setDataSourceClassName(driverclass);
		dataSource.addDataSourceProperty("url", urlTenant+"sampledb");
		dataSource.addDataSourceProperty("user", username);
		dataSource.addDataSourceProperty("password", password);
		return dataSource;
	}

	public DataSource tenantTwo() {
		HikariDataSource dataSource = new HikariDataSource();
		dataSource.setInitializationFailTimeout(0);
		dataSource.setMaximumPoolSize(5);
		dataSource.setDataSourceClassName(driverclass);
		dataSource.addDataSourceProperty("url", urlTenant+"sampledb2");
		dataSource.addDataSourceProperty("user", username);
		dataSource.addDataSourceProperty("password", password);
		return dataSource;
	}*/

	private static Properties getDefaultProperties() {
		Properties defaultProperties = new Properties();
		// Set sane Spring Hibernate properties:
		defaultProperties.put("spring.jpa.show-sql", "true");
		defaultProperties.put("spring.jpa.hibernate.naming.physical-strategy", "org.hibernate.boot.model.naming.PhysicalNamingStrategyStandardImpl");
		defaultProperties.put("spring.datasource.initialize", "false");
		// Prevent JPA from trying to Auto Detect the Database:
		defaultProperties.put("spring.jpa.database", "postgresql");
		// Prevent Hibernate from Automatic Changes to the DDL Schema:
		defaultProperties.put("spring.jpa.hibernate.ddl-auto", "none");
		return defaultProperties;
	}
}