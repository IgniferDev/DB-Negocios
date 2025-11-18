USE PROJECT_SUPPORT_SYSTEM;
GO

IF OBJECT_ID('dbo.vw_cubo','V') IS NOT NULL
  DROP VIEW dbo.vw_cubo;
GO

CREATE VIEW dbo.vw_cubo AS
SELECT
  d.full_date AS fecha,
  DATEPART(YEAR, d.full_date) AS Año,
  DATEPART(QUARTER, d.full_date) AS Trimestre,
  DATENAME(MONTH, d.full_date) AS Mes,
  dp.project_id,
  dp.project_name,
  f.total_hours,    -- o derive a rows granulares si no has daily
  f.total_cost,
  f.total_revenue AS Ventas,
  f.total_errors AS Issues
FROM fact_project f
JOIN dim_project dp ON f.project_id = dp.project_id
JOIN dim_date d ON f.start_date_id = d.date_id  -- si tienes start/end, ajustar
-- NOTA: Esta vista es un ejemplo. Idealmente la vista debe ser construida por el ETL con registros por día o por transacción.
GO


