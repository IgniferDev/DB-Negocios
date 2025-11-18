-- Example create statements for dim_date, dim_project, dim_client, dim_team, fact_project
USE PROJECT_SUPPORT_SYSTEM;
GO

IF OBJECT_ID('dim_date') IS NULL
CREATE TABLE dim_date (
  date_id INT PRIMARY KEY,
  full_date DATE,
  year INT,
  quarter INT,
  month INT,
  day INT,
  weekday VARCHAR(20)
);
GO

IF OBJECT_ID('dim_project') IS NULL
CREATE TABLE dim_project (
  project_id INT PRIMARY KEY,
  project_name VARCHAR(255),
  priority VARCHAR(50),
  budget NUMERIC(12,2),
  start_date VARCHAR(50),
  end_date VARCHAR(50)
);
GO

IF OBJECT_ID('dim_client') IS NULL
CREATE TABLE dim_client (
  client_id INT PRIMARY KEY,
  client_name VARCHAR(255),
  industry VARCHAR(100),
  tier VARCHAR(50)
);
GO

IF OBJECT_ID('dim_team') IS NULL
CREATE TABLE dim_team (
  team_id INT PRIMARY KEY,
  team_name VARCHAR(255),
  description VARCHAR(255)
);
GO

IF OBJECT_ID('fact_project') IS NULL
CREATE TABLE fact_project (
  fact_id BIGINT IDENTITY(1,1) PRIMARY KEY,
  project_id INT NOT NULL,
  client_id INT NOT NULL,
  team_id INT NOT NULL,
  start_date_id INT NOT NULL,
  end_date_id INT NOT NULL,
  total_hours NUMERIC(12,2),
  total_cost NUMERIC(12,2),
  total_revenue NUMERIC(12,2),
  total_errors INT
);
GO
