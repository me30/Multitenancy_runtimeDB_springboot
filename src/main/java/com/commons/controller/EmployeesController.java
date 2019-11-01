package com.commons.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.commons.entity.Employee;
import com.commons.service.EmployeeService;

@RestController
@RequestMapping(value="/employees")
public class EmployeesController {

	@Autowired
	private EmployeeService employeeService;

	@RequestMapping(value="/create", method=RequestMethod.POST)
	public ResponseEntity<Employee> create(@RequestBody Employee emp){
		Employee save = employeeService.saveEmployee(emp);
		return new ResponseEntity<Employee>(save, HttpStatus.CREATED);

	} 

}
