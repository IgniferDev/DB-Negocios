-- Script_A_CREATE_PROJECT_MANAGE.sql
IF DB_ID('PROJECT_MANAGE') IS NULL
    CREATE DATABASE PROJECT_MANAGE;
GO
USE PROJECT_MANAGE;
GO

-- Tabla Employees
IF OBJECT_ID('dbo.employee') IS NOT NULL DROP TABLE dbo.employee;
CREATE TABLE dbo.employee (
    employee_id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    role VARCHAR(50),
    hire_date DATE,
    hourly_cost DECIMAL(12,2),
    capacity_weekly DECIMAL(12,2)
);

-- Tabla Teams
IF OBJECT_ID('dbo.[team]') IS NOT NULL DROP TABLE dbo.[team];
CREATE TABLE dbo.[team] (
    team_id INT PRIMARY KEY,
    team_name VARCHAR(255) NOT NULL,
    description VARCHAR(255)
);

-- Tabla Clientes
IF OBJECT_ID('dbo.client') IS NOT NULL DROP TABLE dbo.client;
CREATE TABLE dbo.client (
    client_id INT PRIMARY KEY,
    client_name VARCHAR(255) NOT NULL,
    industry VARCHAR(100),
    tier VARCHAR(50)
);

-- Tabla Stage
IF OBJECT_ID('dbo.[stage]') IS NOT NULL DROP TABLE dbo.[stage];
CREATE TABLE dbo.[stage] (
    stage_id INT PRIMARY KEY,
    stage_name VARCHAR(255) NOT NULL,
    description VARCHAR(255)
);

-- Tabla Project (sin FK a project_stage aún)
IF OBJECT_ID('dbo.[project]') IS NOT NULL DROP TABLE dbo.[project];
CREATE TABLE dbo.[project] (
    project_id INT PRIMARY KEY,
    project_name VARCHAR(255) NOT NULL,
    description VARCHAR(500),
    start_date DATE,
    end_date DATE,
    client_id INT NOT NULL,
    team_id INT NOT NULL,
    budget DECIMAL(12,2),
    priority VARCHAR(50),
    current_stage_id BIGINT NULL
);

-- Tablas con IDENTITY PK
IF OBJECT_ID('dbo.project_stage') IS NOT NULL DROP TABLE dbo.project_stage;
CREATE TABLE dbo.project_stage (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    project_id INT NOT NULL,
    stage_id INT NOT NULL,
    start_date DATE,
    end_date DATE,
    CONSTRAINT FK_project_stage_project FOREIGN KEY (project_id) REFERENCES dbo.project(project_id),
    CONSTRAINT FK_project_stage_stage FOREIGN KEY (stage_id) REFERENCES dbo.[stage](stage_id)
);

-- Add FK current_stage_id after project_stage exists
ALTER TABLE dbo.project
    ADD CONSTRAINT FK_project_current_stage FOREIGN KEY (current_stage_id) REFERENCES dbo.project_stage(id);

-- Team member
IF OBJECT_ID('dbo.team_member') IS NOT NULL DROP TABLE dbo.team_member;
CREATE TABLE dbo.team_member (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    team_id INT NOT NULL,
    employee_id INT NOT NULL,
    assigned_date DATE,
    role_in_team VARCHAR(50),
    CONSTRAINT FK_team_member_team FOREIGN KEY (team_id) REFERENCES dbo.[team](team_id),
    CONSTRAINT FK_team_member_employee FOREIGN KEY (employee_id) REFERENCES dbo.employee(employee_id)
);

-- Time Log
IF OBJECT_ID('dbo.time_log') IS NOT NULL DROP TABLE dbo.time_log;
CREATE TABLE dbo.time_log (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    project_id INT NOT NULL,
    employee_id INT NOT NULL,
    hours DECIMAL(12,2),
    cost DECIMAL(12,2),
    log_date DATE,
    CONSTRAINT FK_time_log_project FOREIGN KEY (project_id) REFERENCES dbo.project(project_id),
    CONSTRAINT FK_time_log_employee FOREIGN KEY (employee_id) REFERENCES dbo.employee(employee_id)
);

-- Issues
IF OBJECT_ID('dbo.[issue]') IS NOT NULL DROP TABLE dbo.[issue];
CREATE TABLE dbo.[issue] (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    project_id INT NOT NULL,
    project_stage_id BIGINT NOT NULL,
    severity VARCHAR(50),
    concept VARCHAR(255),
    error_message NVARCHAR(MAX),
    date_reported DATE,
    CONSTRAINT FK_issue_project FOREIGN KEY (project_id) REFERENCES dbo.project(project_id),
    CONSTRAINT FK_issue_project_stage FOREIGN KEY (project_stage_id) REFERENCES dbo.project_stage(id)
);

-- Financials
IF OBJECT_ID('dbo.financials') IS NOT NULL DROP TABLE dbo.financials;
CREATE TABLE dbo.financials (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    project_id INT NOT NULL,
    amount DECIMAL(12,2),
    type VARCHAR(50),
    currency VARCHAR(10),
    date_recorded DATE,
    CONSTRAINT FK_financials_project FOREIGN KEY (project_id) REFERENCES dbo.project(project_id)
);

PRINT 'PROJECT_MANAGE schema created.';
GO
