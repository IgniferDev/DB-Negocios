-- Script_B_CREATE_PROJECT_SUPPORT_SYSTEM.sql
IF DB_ID('PROJECT_SUPPORT_SYSTEM') IS NULL
    CREATE DATABASE PROJECT_SUPPORT_SYSTEM;
GO
USE PROJECT_SUPPORT_SYSTEM;
GO

-- Dim Project
IF OBJECT_ID('dbo.dim_project') IS NOT NULL DROP TABLE dbo.dim_project;
CREATE TABLE dbo.dim_project (
    project_id INT PRIMARY KEY, -- natural key from source
    project_name VARCHAR(255) NOT NULL,
    priority VARCHAR(50),
    budget DECIMAL(12,2)
);

-- Dim Client
IF OBJECT_ID('dbo.dim_client') IS NOT NULL DROP TABLE dbo.dim_client;
CREATE TABLE dbo.dim_client (
    client_id INT PRIMARY KEY,
    client_name VARCHAR(255) NOT NULL,
    industry VARCHAR(100),
    tier VARCHAR(50)
);

-- Dim Team
IF OBJECT_ID('dbo.dim_team') IS NOT NULL DROP TABLE dbo.dim_team;
CREATE TABLE dbo.dim_team (
    team_id INT PRIMARY KEY,
    team_name VARCHAR(255) NOT NULL,
    description VARCHAR(255)
);

-- Dim Date
IF OBJECT_ID('dbo.dim_date') IS NOT NULL DROP TABLE dbo.dim_date;
CREATE TABLE dbo.dim_date (
    date_id INT PRIMARY KEY, -- format YYYYMMDD
    full_date DATE NOT NULL,
    [year] INT,
    quarter INT,
    [month] INT,
    [day] INT,
    weekday VARCHAR(20)
);

-- Fact Project
IF OBJECT_ID('dbo.fact_project') IS NOT NULL DROP TABLE dbo.fact_project;
CREATE TABLE dbo.fact_project (
    fact_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    project_id INT NOT NULL,
    client_id INT NOT NULL,
    team_id INT NOT NULL,
    start_date_id INT NOT NULL,
    end_date_id INT NOT NULL,
    total_hours DECIMAL(12,2),
    total_cost DECIMAL(12,2),
    total_revenue DECIMAL(12,2),
    total_errors INT,
    CONSTRAINT FK_fact_project_dim_project FOREIGN KEY (project_id) REFERENCES dbo.dim_project(project_id),
    CONSTRAINT FK_fact_project_dim_client FOREIGN KEY (client_id) REFERENCES dbo.dim_client(client_id),
    CONSTRAINT FK_fact_project_dim_team FOREIGN KEY (team_id) REFERENCES dbo.dim_team(team_id),
    CONSTRAINT FK_fact_project_dim_start_date FOREIGN KEY (start_date_id) REFERENCES dbo.dim_date(date_id),
    CONSTRAINT FK_fact_project_dim_end_date FOREIGN KEY (end_date_id) REFERENCES dbo.dim_date(date_id)
);

PRINT 'PROJECT_SUPPORT_SYSTEM schema created.';
GO
