package com.commons.controller;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.commons.core.TargetDataSources;
import com.commons.model.TenantModel;
import com.zaxxer.hikari.HikariDataSource;

@RestController
@RequestMapping(value="/tenant")
public class TenantController {

	@Autowired
	AbstractRoutingDataSource dataSource;

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
	
	@PersistenceContext
	EntityManager entityManager;

	@RequestMapping(value="/create", method=RequestMethod.POST)
	public ResponseEntity<?> create(@RequestBody TenantModel model) throws Exception{

		//create database..
		createDatabase(model);

		//create table 
		executeScriptUsingStatement(model);

		//Register New db with application database.
		registerNewDb(model);

		TargetDataSources.getTargetDataSources().put(model.getTenantID(), tenantRuntime(model.getDbname()));
		dataSource.setTargetDataSources(TargetDataSources.getTargetDataSources());
		dataSource.afterPropertiesSet(); 

		return new ResponseEntity<String>("OK", HttpStatus.CREATED);
	} 

	private void registerNewDb(TenantModel model) throws Exception {
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
			String sql = "INSERT INTO applicationdb (tenant_id,database_name) VALUES (\'"+model.getTenantID()+"\', \'"+model.getDbname()+"\')";
			stmt.executeUpdate(sql);
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

	}

	private void createDatabase(TenantModel model) throws Exception {
		Class.forName(driverclass);
		Connection conn = DriverManager.getConnection(urlTenant, username, password);
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("create database " + model.getDbname());

		if (conn != null && !conn.isClosed()) {
			conn.close();
		}
	}

	private void executeScriptUsingStatement(TenantModel model) throws Exception {
		String scriptFilePath = "create.sql";
		BufferedReader reader = null;
		Connection con = null;
		Statement statement = null;
		try {
			// load driver class for mysql
			Class.forName(driverclass);
			// create connection
			con = DriverManager.getConnection(urlTenant+model.getDbname(), username, password);
			// create statement object
			statement = con.createStatement();
			// initialize file reader
			reader = new BufferedReader(new FileReader(getClass().getClassLoader().getResource(scriptFilePath).getFile()));
			String line = null;
			// read script line by line
			while ((line = reader.readLine()) != null) {
				// execute query
				statement.execute(line);
			}
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

}
