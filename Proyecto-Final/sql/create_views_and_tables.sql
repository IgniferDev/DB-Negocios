USE PROJECT_SUPPORT_SYSTEM;
GO

-- model_params table (for storing Rayleigh params)
IF OBJECT_ID('dbo.model_params','U') IS NULL
CREATE TABLE dbo.model_params (
  model_name VARCHAR(50) PRIMARY KEY,
  sigma FLOAT,
  mean FLOAT,
  trained_on DATETIME
);
GO

-- prediction audit
IF OBJECT_ID('dbo.prediction_audit','U') IS NULL
CREATE TABLE dbo.prediction_audit (
  run_ts DATETIME,
  user_token VARCHAR(100),
  project_hours FLOAT,
  predicted FLOAT,
  p10 FLOAT,
  p90 FLOAT
);
GO

-- fact_time_log_daily (aggregated from source time_log by ETL)
IF OBJECT_ID('dbo.fact_time_log_daily','U') IS NULL
CREATE TABLE dbo.fact_time_log_daily (
  project_id INT,
  date_id INT,       -- YYYYMMDD
  total_hours NUMERIC(12,2),
  total_cost NUMERIC(12,2),
  PRIMARY KEY (project_id, date_id)
);
GO

-- vw_cubo: sample view combining daily facts with dims (adjust if your dims differ)
IF OBJECT_ID('dbo.vw_cubo','V') IS NOT NULL
DROP VIEW dbo.vw_cubo;
GO

CREATE VIEW dbo.vw_cubo AS
SELECT
    d.full_date AS fecha,
    DATEPART(YEAR, d.full_date) AS Año,
    DATEPART(QUARTER, d.full_date) AS Trimestre,
    DATENAME(MONTH, d.full_date) AS Mes,
    p.project_id,
    p.project_name,
    t.total_hours,
    t.total_cost,
    ISNULL(f.total_revenue,0) AS Ventas,
    ISNULL(fp.total_errors,0) AS Issues
FROM fact_time_log_daily t
LEFT JOIN dim_date d ON t.date_id = d.date_id
LEFT JOIN dim_project p ON t.project_id = p.project_id
LEFT JOIN (SELECT project_id, SUM(total_revenue) AS total_revenue FROM fact_project GROUP BY project_id) f ON p.project_id = f.project_id
LEFT JOIN (SELECT project_id, SUM(total_errors) AS total_errors FROM fact_project GROUP BY project_id) fp ON p.project_id = fp.project_id;
GO
