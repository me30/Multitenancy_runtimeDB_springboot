package com.commons.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.commons.entity.Employee;

public interface EmployeeRepository extends JpaRepository<Employee, Long>{

	public Employee findByPassword(String password);

	public Employee findById(long id);

	@Query("select e from Employee e where e.valid=1")
	public List<Employee> findAllEmployees();
}


