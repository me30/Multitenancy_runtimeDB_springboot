package com.commons;

import java.io.BufferedReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
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

	@Value("${app.db}")
	private String appdb;

	@Bean
	public DataSource dataSource() throws Exception {
		AbstractRoutingDataSource dataSource = new TenantAwareRoutingSource();

		//Data source and tenant register.... 
		Map<Object,Object> targetDataSources = TargetDataSources.getTargetDataSources();

		BufferedReader reader = null;
		Connection con = null;
		Statement stmt = null;
		try {
			// load driver class for mysql
			Class.forName(driverclass);
			// create connection
			con = DriverManager.getConnection(appdb, username, password);
			// create statement object
			stmt = con.createStatement();
			String sql = "SELECT id, tenant_id, database_name FROM applicationdb";
			ResultSet rs = stmt.executeQuery(sql);
			//STEP 5: Extract data from result set
			while(rs.next()){
				//Retrieve by column name
				int id  = rs.getInt("id");
				String tenant_id = rs.getString("tenant_id");
				String database_name = rs.getString("database_name");

				//Display values
				System.out.print("ID:" + id +"  tenant_id:"+tenant_id+"  database_name:"+database_name);
				targetDataSources.put(tenant_id, tenantRuntime(database_name));
			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// close file reader
			if (reader != null) {
				reader.close();
			}
			// close db connection
			if (con != null) {
				con.close();
			}
		}



		//end..

		dataSource.setTargetDataSources(targetDataSources);
		dataSource.afterPropertiesSet();
		return dataSource;
	}

	public DataSource tenantRuntime(String name) {
		HikariDataSource dataSource = new HikariDataSource();
		dataSource.setInitializationFailTimeout(0);
		dataSource.setMaximumPoolSize(5);
		dataSource.setDataSourceClassName(driverclass);
		dataSource.addDataSourceProperty("url", urlTenant+name);
		dataSource.addDataSourceProperty("user", username);
		dataSource.addDataSourceProperty("password", password);
		return dataSource;
	}


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