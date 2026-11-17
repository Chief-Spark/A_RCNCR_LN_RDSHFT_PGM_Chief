-- HU SLCOPRBA-1820 | Subtarea SLCOPRBA-1824 (PGM) - UNLOAD Calculo RECONOCER
-- ============================================================
-- sp_novedades_unload_una  (Reconocer / Novedades hacia la cuenta Batch)
-- Helper del Exportador de Novedades (GUIA-UNLOAD-Y-CARGA, Paso 1): hace UN
-- UNLOAD de una tabla staging ya materializada a
--   s3://<bucket>/<data_prefix>/<tabla>/   (CSV HEADER GZIP ALLOWOVERWRITE)
-- p.ej. s3://dev-647096294147-rcncr-ws5-triggers-reconocer-s3/nov/<run_id>/data/<tabla>/
-- HEADER se conserva: la primera linea nombra las columnas (pk, sk, ...) para
-- que la importacion a DynamoDB y el Bureau mapeen por nombre.
--
-- Objeto en bdm_datos; idempotente (CREATE OR REPLACE PROCEDURE). NONATOMIC
-- (UNLOAD no admite transaccion implicita). UTF-8 sin BOM. ASCII sin acentos.
-- Nunca GRANT ... TO PUBLIC.
--
-- El conteo NO lo devuelve este helper: el orquestador cuenta la staging con
-- SELECT COUNT(*) antes de llamar, igual que el Exportador_GEO cuenta
-- stg_geo_insumo. Asi este helper no necesita INOUT y la responsabilidad del
-- conteo queda en un solo sitio.
--
-- in_tabla_stg = nombre calificado de la tabla staging (esquema.tabla), ya
--   con las columnas pk, sk y el resto en el orden del CSV.
-- ============================================================

CREATE OR REPLACE PROCEDURE bdm_datos.sp_novedades_unload_una(
    in_tabla_stg    VARCHAR,   -- staging calificada: 'bdm_tempo.stg_nov_export_...'
    in_bucket       VARCHAR,
    in_data_prefix  VARCHAR,   -- <base>/<run_id>/data
    in_tabla        VARCHAR,   -- nombre logico de la tabla (carpeta destino)
    in_iam_role     VARCHAR,
    in_region       VARCHAR
)
NONATOMIC
LANGUAGE plpgsql
AS $$
DECLARE
  v_dest  VARCHAR(1200);
  v_sql   VARCHAR(MAX);
BEGIN
  v_dest := 's3://' || in_bucket || '/' || in_data_prefix || '/' || in_tabla || '/';

  v_sql :=
       'UNLOAD (''SELECT * FROM ' || in_tabla_stg || ''') '
    || 'TO ''' || v_dest || ''' '
    || 'IAM_ROLE ''' || in_iam_role || ''' '
    || 'FORMAT AS CSV HEADER GZIP ALLOWOVERWRITE '
    || 'REGION ''' || in_region || '''';

  EXECUTE v_sql;
END;
$$;
