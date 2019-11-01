CREATE database sampledb;
CREATE database sampledb1;

CREATE TABLE employee
    (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        password VARCHAR(255),
        descr VARCHAR(255),
        gross_salary_month DOUBLE PRECISION,
        health_insurance_share DOUBLE PRECISION,
        surcharge DOUBLE PRECISION,
        type VARCHAR(255),
        working_contract VARCHAR(255),
        contract_hours_month DOUBLE PRECISION,
        limit_hours_month DOUBLE PRECISION,
        ref_id bigint,
        valid bigint
    );
    