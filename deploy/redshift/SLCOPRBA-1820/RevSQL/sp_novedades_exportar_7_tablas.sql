-- HU SLCOPRBA-1820 | Subtarea SLCOPRBA-1824 (PGM) - UNLOAD Calculo RECONOCER
-- Rollback de sp_novedades_exportar_7_tablas.sql (helper en bdm_datos)
-- Redshift NO soporta DROP PROCEDURE IF EXISTS (reglas #12/#14): firma EXACTA.
DROP PROCEDURE bdm_datos.sp_novedades_exportar_7_tablas(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR);
