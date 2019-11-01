package com.commons.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import org.springframework.beans.BeanUtils;
@Entity
public class Employee implements Serializable{
	/**
	 * 
	 */ 
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	private String name;

	@Column(unique=true)
	private String password;

	private String descr;
	private double gross_salary_month;
	private double health_insurance_share;
	private double surcharge;
	private String type;
	private String working_contract;
	private double contract_hours_month;
	private double limit_hours_month;
	private long ref_id;
	private int valid;

	public Employee() {
	}

	public Employee(String name, String password, String descr, double gross_salary_month, double health_insurance_share, double surcharge, String type, String working_contract, double contract_hours_month, double limit_hours_month, long ref_id, int valid) {
		this.name = name;
		this.password = password;
		this.descr = descr;
		this.gross_salary_month = gross_salary_month;
		this.health_insurance_share = health_insurance_share;
		this.surcharge = surcharge;
		this.type = type;
		this.working_contract = working_contract;
		this.contract_hours_month = contract_hours_month;
		this.limit_hours_month = limit_hours_month;
		this.ref_id = ref_id;
		this.valid = valid;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public long getRef_id() {
		return ref_id;
	}

	public void setRef_id(long ref_id) {
		this.ref_id = ref_id;
	}

	public int getValid() {
		return valid;
	}

	public void setValid(int valid) {
		this.valid = valid;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescr() {
		return descr;
	}

	public void setDescr(String descr) {
		this.descr = descr;
	}

	public double getGross_salary_month() {
		return gross_salary_month;
	}

	public void setGross_salary_month(double gross_salary_month) {
		this.gross_salary_month = gross_salary_month;
	}

	public double getHealth_insurance_share() {
		return health_insurance_share;
	}

	public void setHealth_insurance_share(double health_insurance_share) {
		this.health_insurance_share = health_insurance_share;
	}

	public double getSurcharge() {
		return surcharge;
	}

	public void setSurcharge(double surcharge) {
		this.surcharge = surcharge;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getWorking_contract() {
		return working_contract;
	}

	public void setWorking_contract(String working_contract) {
		this.working_contract = working_contract;
	}

	public double getContract_hours_month() {
		return contract_hours_month;
	}

	public void setContract_hours_month(double contract_hours_month) {
		this.contract_hours_month = contract_hours_month;
	}

	public double getLimit_hours_month() {
		return limit_hours_month;
	}

	public void setLimit_hours_month(double limit_hours_month) {
		this.limit_hours_month = limit_hours_month;
	}

	public Employee createCloneWithoutId(){
		Employee em = new Employee();
		BeanUtils.copyProperties(this, em);
		em.setId(null);
		return em;
	}

}
