--  P O N K I  ##
-- Conteos y muetras (evidencia) ##
USE PROJECT_MANAGE;
GO
SELECT 'employee' AS tabla, COUNT(*) AS cnt FROM dbo.employee;
SELECT 'team' AS tabla, COUNT(*) AS cnt FROM dbo.[team];
SELECT 'client' AS tabla, COUNT(*) AS cnt FROM dbo.client;
SELECT 'project' AS tabla, COUNT(*) AS cnt FROM dbo.project;
SELECT 'time_log' AS tabla, COUNT(*) AS cnt FROM dbo.time_log;
SELECT 'issue' AS tabla, COUNT(*) AS cnt FROM dbo.[issue];

SELECT TOP 5 * FROM dbo.employee;
SELECT TOP 5 * FROM dbo.project;
SELECT TOP 10 project_id, log_date, hours, cost FROM dbo.time_log ORDER BY log_date DESC;
SELECT TOP 10 * FROM dbo.[issue] ORDER BY date_reported DESC;



-- Ejecutar en Ponki (usa tablas transaccionales) 
-- ¿Qué mide? Horas totales trabajadas y costo real asociado por proyecto; compara con el presupuesto para ver desviaciones.
USE PROJECT_MANAGE;
GO

SELECT 
  p.project_id, 
  p.project_name,
  COALESCE(SUM(t.hours),0) AS horas_consumidas,
  COALESCE(SUM(t.cost),0) AS costo_real,
  p.budget,
  CASE 
    WHEN p.budget IS NULL OR p.budget = 0 THEN NULL 
    ELSE ROUND((COALESCE(SUM(t.cost),0) - p.budget) / p.budget * 100.0, 2) 
  END AS pct_variacion_presupuesto
FROM dbo.project p
LEFT JOIN dbo.time_log t ON p.project_id = t.project_id
GROUP BY p.project_id, p.project_name, p.budget
ORDER BY horas_consumidas DESC;


-- Utilización por empleado (último mes) en Ponki
-- ¿Qué mide? Porcentaje de utilización del empleado en el último mes: horas trabajadas vs capacidad esperada (capacity_weekly × 4 semanas).
USE PROJECT_MANAGE;
GO

DECLARE @start DATE = DATEADD(month, -1, CAST(GETDATE() AS DATE));
DECLARE @end   DATE = CAST(GETDATE() AS DATE);

SELECT 
  e.employee_id, 
  e.name,
  COALESCE(SUM(t.hours),0) AS horas_trabajadas,
  e.capacity_weekly * 4.0 AS capacidad_mes,
  CASE 
    WHEN e.capacity_weekly * 4.0 = 0 THEN NULL
    ELSE ROUND(COALESCE(SUM(t.hours),0) / (e.capacity_weekly * 4.0) * 100.0, 2)
  END AS utilization_pct
FROM dbo.employee e
LEFT JOIN dbo.time_log t 
  ON e.employee_id = t.employee_id 
  AND t.log_date BETWEEN @start AND @end
GROUP BY e.employee_id, e.name, e.capacity_weekly
ORDER BY utilization_pct DESC;



-- Issues por severidad y proyecto
-- ¿Qué mide? Número de incidencias (issues) por severidad y proyecto — ayuda a detectar proyectos/ etapas con mayor riesgo de calidad.

USE PROJECT_MANAGE;
GO

SELECT 
  p.project_id, 
  p.project_name, 
  i.severity, 
  COUNT(*) AS total_issues
FROM dbo.[issue] i
JOIN dbo.project p ON p.project_id = i.project_id
GROUP BY p.project_id, p.project_name, i.severity
ORDER BY p.project_id, total_issues DESC;


-- Burn rate (por día)
-- ¿Qué mide? Gasto promedio diario del proyecto (burn rate) — útil para controlar ritmo de consumo del presupuesto.

USE PROJECT_MANAGE;
GO

SELECT 
  p.project_id,
  p.project_name,
  COALESCE(SUM(t.cost),0) AS total_cost,
  CASE WHEN DATEDIFF(day, p.start_date, p.end_date) <= 0 THEN NULL 
       ELSE DATEDIFF(day, p.start_date, p.end_date) END AS duration_days,
  CASE 
    WHEN DATEDIFF(day, p.start_date, p.end_date) <= 0 THEN NULL
    ELSE ROUND(COALESCE(SUM(t.cost),0) / NULLIF(DATEDIFF(day, p.start_date, p.end_date),0), 2)
  END AS burn_rate_per_day
FROM dbo.project p
LEFT JOIN dbo.time_log t ON p.project_id = t.project_id
GROUP BY p.project_id, p.project_name, p.start_date, p.end_date
ORDER BY burn_rate_per_day DESC;





--  K I R Y  ##
-- Conteos y hechos (evidencia) ##

USE PROJECT_SUPPORT_SYSTEM;
GO
SELECT 'dim_client' AS tabla, COUNT(*) AS cnt FROM dbo.dim_client;
SELECT 'dim_team'   AS tabla, COUNT(*) AS cnt FROM dbo.dim_team;
SELECT 'dim_project'AS tabla, COUNT(*) AS cnt FROM dbo.dim_project;
SELECT 'dim_date' AS tabla, COUNT(*) AS cnt FROM dbo.dim_date;
SELECT 'fact_project' AS tabla, COUNT(*) AS cnt FROM dbo.fact_project;

SELECT TOP 20 project_id, total_hours, total_cost, total_revenue, total_errors
FROM dbo.fact_project
ORDER BY total_hours DESC;

SELECT TOP 10 * FROM dbo.etl_audit ORDER BY run_id DESC;



-- Ejecutar en Kiry (usa hecho ya calculado)
-- Si ya corriste el ETL y poblaste fact_project, consulta más rápida:

USE PROJECT_SUPPORT_SYSTEM;
GO

SELECT 
  f.project_id,
  d.project_name,
  f.total_hours AS horas_consumidas,
  f.total_cost  AS costo_real,
  d.budget,
  CASE 
    WHEN d.budget IS NULL OR d.budget = 0 THEN NULL
    ELSE ROUND((f.total_cost - d.budget) / d.budget * 100.0, 2)
  END AS pct_variacion_presupuesto
FROM dbo.fact_project f
JOIN dbo.dim_project d ON f.project_id = d.project_id
ORDER BY f.total_hours DESC;


-- Burn rate (por día) Ejecutar en Kiry (usando hecho)
-- ¿Qué mide? Gasto promedio diario del proyecto (burn rate) — útil para controlar ritmo de consumo del presupuesto.
USE PROJECT_SUPPORT_SYSTEM;
GO

SELECT 
  f.project_id,
  d.project_name,
  f.total_cost AS total_cost,
  -- recuperar duración desde dim_project si contiene fechas; si no, puedes guardar start/end en dim_project
  d.budget,
  -- si dim_project tiene start_date_id / end_date_id, convertir:
  -- CONVERT(date, CONVERT(varchar(8), d.start_date_id), 112) etc.
  ROUND(
    CASE 
      WHEN (CONVERT(INT, (CONVERT(VARCHAR(8), d.end_date_id))) - CONVERT(INT, (CONVERT(VARCHAR(8), d.start_date_id)))) <= 0 THEN NULL
      ELSE f.total_cost / NULLIF((CONVERT(INT, (CONVERT(VARCHAR(8), d.end_date_id))) - CONVERT(INT, (CONVERT(VARCHAR(8), d.start_date_id)))),0)
    END, 2) AS burn_rate_per_day
FROM dbo.fact_project f
JOIN dbo.dim_project d ON f.project_id = d.project_id
ORDER BY burn_rate_per_day DESC;
