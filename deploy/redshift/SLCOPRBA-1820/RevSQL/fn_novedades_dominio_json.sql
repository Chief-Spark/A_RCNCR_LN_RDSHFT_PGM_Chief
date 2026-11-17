-- HU SLCOPRBA-1820 | Subtarea SLCOPRBA-1824 (PGM) - UNLOAD Calculo RECONOCER
-- Rollback de fn_novedades_dominio_json.sql
-- Redshift NO soporta DROP FUNCTION IF EXISTS (regla #10): firma EXACTA. Si el
-- objeto no existe, el deploy de rollback asume el fallo de esa linea.
DROP FUNCTION bdm_datos.fn_novedades_dominio_json(VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN);
