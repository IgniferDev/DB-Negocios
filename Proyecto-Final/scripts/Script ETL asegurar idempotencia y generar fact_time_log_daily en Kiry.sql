USE PROJECT_SUPPORT_SYSTEM;
GO

IF OBJECT_ID('dbo.fact_time_log_daily') IS NULL
CREATE TABLE dbo.fact_time_log_daily (
  project_id INT,
  date_id INT,       -- YYYYMMDD
  total_hours NUMERIC(12,2),
  total_cost NUMERIC(12,2),
  PRIMARY KEY (project_id, date_id)
);
GO
