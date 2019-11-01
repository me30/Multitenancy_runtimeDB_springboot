# Multitenancy:

A multitenant application is a shared resource that allows separate users, or "tenants," to view the application as though it was their own. A typical scenario that lends itself to a multitenant application is one in which all users of the application may wish to customize the user experience but otherwise have the same basic business requirements

# Multitenant Models:
There are several models to achieve multitenancy in an application:

 _1)Database per Tenant_: Each Tenant has its own database and is isolated from other tenants.

 _2)Shared database, Separate Schema_: All Tenants share a database, but have their own database schemas and their own tables.

 _3)Shared Database, Shared Schema_: All Tenants share a database and tables. Every table has a Column with the Tenant Identifier, that shows the owner of the row.

**Think about it**

 _1)Database per Tenant_: Every Tenant has its own house.
 
 _2)Shared Database, Separate Schema_: Every Tenant in the same building, but has its own apartment.
 
 _3)Shared Database, Shared Schema_: Everyone is living in the same apartment and all stuff is marked with sticky-notes to show who owns it.

Every model is a trade-off between isolation and resource sharing.

# This Application is used to create database and register new tenant runtime

**create database and register tenantID runtime **

```
curl -X POST \
  http://localhost:8080/tenant/create \
  -H 'cache-control: no-cache' \
  -H 'content-type: application/json' \
  -d '{
	"dbname":"sampledb1",
	"tenantID":"TenantOne"
}'
```

```
curl -X POST \
  http://localhost:8080/tenant/create \
  -H 'cache-control: no-cache' \
  -H 'content-type: application/json' \
  -d '{
	"dbname":"sampledb2",
	"tenantID":"TenantTwo"
}'
```
**TenantOne Testing :**

```
curl -X POST \
  http://localhost:8080/employees/create \
  -H 'cache-control: no-cache' \
  -H 'content-type: application/json' \
  -H 'x-tenantid: TenantOne' \
  -d '{

	"name":"Emp1 tenta two",
	"password":"12345",
	"descr":"Tet description",
	"gross_salary_month":2000,
	"health_insurance_share":2300,
	"surcharge":30,
	"type":"parmanent",
	"working_contract":"Long",
	"contract_hours_month":3,
	"limit_hours_month":3,
	"ref_id":2,
	"valid":0
}'
```
