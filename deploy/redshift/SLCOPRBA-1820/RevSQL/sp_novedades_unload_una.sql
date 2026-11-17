-- HU SLCOPRBA-1820 | Subtarea SLCOPRBA-1824 (PGM) - UNLOAD Calculo RECONOCER
-- Rollback de sp_novedades_unload_una.sql
-- Redshift NO soporta DROP PROCEDURE IF EXISTS (reglas #12/#14): firma EXACTA
-- del CREATE, en el mismo orden de parametros.
DROP PROCEDURE bdm_datos.sp_novedades_unload_una(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR);
