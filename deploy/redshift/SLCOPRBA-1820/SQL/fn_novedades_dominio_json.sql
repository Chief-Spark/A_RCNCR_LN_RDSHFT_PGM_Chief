-- HU SLCOPRBA-1820 | Subtarea SLCOPRBA-1824 (PGM) - UNLOAD Calculo RECONOCER
-- ============================================================
-- fn_novedades_dominio_json  (Reconocer / Novedades hacia la cuenta Batch)
-- Helper del Exportador de Novedades (GUIA-UNLOAD-Y-CARGA, Paso 2): devuelve
-- UNA entrada del arreglo "dominios" del CTL final:
--   {"nombre":"<tabla>","s3_path":"s3://.../data/<tabla>/","registros_esperados":<n>}
-- con una coma delante cuando no es la primera (in_con_coma = true), de modo
-- que el orquestador concatene sin preocuparse del separador.
--
-- Funcion SQL pura en bdm_datos; no toca tablas. STABLE. UTF-8 sin BOM. ASCII.
-- Nunca GRANT ... TO PUBLIC.
-- ============================================================

CREATE OR REPLACE FUNCTION bdm_datos.fn_novedades_dominio_json(
    in_tabla        VARCHAR,
    in_bucket       VARCHAR,
    in_data_prefix  VARCHAR,
    in_conteo       BIGINT,
    in_con_coma     BOOLEAN
)
RETURNS VARCHAR(1024)
STABLE
AS $$
    SELECT CASE WHEN $5 THEN ',' ELSE '' END
        || '{'
        || '"nombre":"' || $1 || '",'
        || '"s3_path":"s3://' || $2 || '/' || $3 || '/' || $1 || '/",'
        || '"registros_esperados":' || COALESCE($4, 0)::VARCHAR
        || '}'
$$ LANGUAGE sql;
