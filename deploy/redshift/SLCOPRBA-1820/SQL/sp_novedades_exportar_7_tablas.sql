-- HU SLCOPRBA-1820 | Subtarea SLCOPRBA-1824 (PGM) - UNLOAD Calculo RECONOCER
-- ============================================================
-- sp_novedades_exportar_7_tablas  (Reconocer / Novedades hacia la cuenta Batch)
-- Exportador de Novedades: UNLOAD de las 7 tablas de dominio a
--   s3://<triggers_bucket>/<base>/<run_id>/data/<tabla>/  (CSV HEADER GZIP)
-- y escritura del CTL final
--   s3://<triggers_bucket>/<base>/<run_id>/7_tablas_finish.json
-- con los conteos por tabla. Implementa el Paso 1 y el Paso 2 de
-- GUIA-UNLOAD-Y-CARGA.
--
-- Objeto HELPER en bdm_datos; idempotente (CREATE OR REPLACE PROCEDURE).
-- NONATOMIC: el UNLOAD no admite transaccion implicita (mezclar modos produce
-- P0001). UTF-8 sin BOM. ASCII sin acentos. Nunca GRANT ... TO PUBLIC.
-- Prerrequisitos: _strct desplegado (bdm_datos.nov_config, 40_ddl_nov_config;
--   bdm_datos.nov_export_control, 41_ddl_nov_export_control)
--   y 12b_fn_fw_resolver_param (fw_resolver_param).
--
-- El Framework Batch lo invoca DIRECTO como NIVEL 4 del YAML del proceso
-- RECONOCER (step_procedure: bdm_datos.sp_novedades_exportar_7_tablas), igual
-- que los niveles 1/2/3 apuntan a bdm_datos. Por eso NO hay wrapper en
-- bdm_stage: este orquestador ya expone la firma de 6 VARCHAR del Framework.
--
-- CONTRATO DEL FRAMEWORK BATCH (medido, no supuesto; ver 12b_fn_fw_resolver_param
-- y tf_fw_dynbtch step_function_definition_builder.py:93):
--   El slot 4 (in_nemotecnico) trae el MNEMOTECNICO, no el modo. El modo
--   FULL/DELTA se DERIVA con bdm_datos.fw_resolver_param('MODO', in_nemotecnico)
--   -- NO se reinterpreta el slot a mano. El slot 6 (in_fecha_ejecucion) trae
--   'YYMM'; fw_resolver_param('FECHA', ...) lo normaliza.
--
-- AUTODETECCION DE AMBIENTE (igual que el Exportador_GEO): por CURRENT_AWS_ACCOUNT
--   se resuelve el ambiente contra bdm_datos.nov_config (parametro
--   'aws_account_id') y de ahi salen triggers_bucket, unload_iam_role,
--   s3_base_prefix y region. Ningun bucket/rol se cablea en el cuerpo.
--
-- CONTEO: patron de la casa (Exportador_GEO) -- se materializa cada tabla en
--   una staging de bdm_tempo y se cuenta con SELECT COUNT(*) INTES del UNLOAD.
--   NO se usa pg_last_unload_count() (no existe en Redshift).
--
-- run_id = <YYYYMMDDTHH24MISSZ>_<FULL|DELTA>  (GUIA seccion 2). UNICO e
--   irrepetible: solo se genera cuando de verdad se exporta.
--
-- CONTROL DE EXPORTACION (mismo principio que geo_lote_control del
--   Exportador_GEO; tabla bdm_datos.nov_export_control, 41_ddl_nov_export_control):
--   1) Se calcula la FIRMA de los datos: modo + por cada una de las 7 tablas
--      COUNT(*), MAX(lote) y MAX(fecha_actualizacion).
--   2) Si existe una corrida 'completado' del mismo ambiente con la MISMA
--      firma, los datos ya se exportaron: NO se hace UNLOAD, NO se crea
--      carpeta y el SP termina OK informando el run_id previo.
--   3) Si no, se genera un run_id nuevo, se registra 'en_proceso', se exportan
--      las 7 tablas + CTL y se marca 'completado' (o 'fallido' si algo falla;
--      una corrida fallida no bloquea la siguiente).
--   in_solicitud (execution_id del Framework) se guarda como ejecucion_id
--   para trazabilidad; NO forma parte del run_id.
--
-- EL DELTA EXPORTA SOLO LO NUEVO (2026-11-17). Hasta hoy v_modo no era un
-- predicado en ningun sitio -- solo etiquetaba la carpeta y el CTL -- de modo
-- que un DELTA bajaba las ~363.000 filas enteras igual que un FULL. El
-- consumidor de DynamoDB hace UPSERT por pk/sk, asi que un delta real es lo
-- correcto.
--
-- EL FILTRO ES fecha_actualizacion > frontera, Y FUNCIONA POR COMO CARGA
-- NOVEDADES: el cargador no hace UPDATE en sitio, hace DELETE + INSERT y solo
-- sobre las filas de la ventana (sp_novedades_cargar_mat_ubicacion_edf.sql:36-70,
-- WHERE flag_op IN ('I','A'); flag_op se construye solo para la ventana en
-- sp_novedades_ubicacion_edf.sql:76-85). La fecha se mueve en altas Y en
-- cambios, y NO se mueve en lo que no se toco.
--   > ESTRICTO y no >=: en Unificacion el >= inclusivo hace que lo fechado en
--     la frontera reentre en todos los DELTA para siempre. No se hereda.
--
-- DOS TABLAS SE TRATAN APARTE, Y NO ES CAPRICHO:
--   mat_info_movilidad  es de EVENTOS y su SP la trunca en cada corrida
--     (sp_novedades_movilidad_edf.sql): su contenido YA es el delta. Sin filtro.
--   contactabilidad y punto_interes se arman por JOIN contra mat_info_ubicacion:
--     el filtro va sobre la tabla HIJA (c./p.), nunca sobre la madre, o una
--     ubicacion vieja con contacto nuevo se perderia.
--
-- LO QUE ESTE CAMBIO NO RESUELVE, Y ES PREGUNTA ABIERTA PARA EL CONSUMIDOR:
--   con delta + UPSERT, un BORRADO es invisible. Si una fila desaparece de una
--   mat_*, el delta no la menciona y Dynamo la conserva. Hoy no muerde porque
--   flag_op = 'E' es codigo muerto en el legado y filas_bajas da 0, pero la
--   salida -- un FULL periodico que reconcilie, o un tombstone en el CTL -- es
--   decision del equipo que consume el bucket.
--
-- pk = tipo_identificacion || '#' || numero_identificacion  (nunca vacio).
-- sk = segun tabla (GUIA seccion 3). Filas sin tipo/numero se EXCLUYEN.
-- ============================================================

CREATE OR REPLACE PROCEDURE bdm_datos.sp_novedades_exportar_7_tablas(
    in_solicitud       VARCHAR,   -- execution_id del Framework -> nov_export_control.ejecucion_id
    in_nit_suscriptor  VARCHAR,   -- no se usa; parte de la firma del Framework
    in_path_archivo    VARCHAR,   -- no se usa; parte de la firma del Framework
    in_nemotecnico     VARCHAR,   -- MNEMOTECNICO -> MODO via fw_resolver_param
    in_id_facturacion  VARCHAR,   -- LOTE (no usado por el export; parte de firma)
    in_fecha_ejecucion VARCHAR    -- FECHA 'YYMM' (no usada por el export)
)
NONATOMIC
LANGUAGE plpgsql
AS $$
DECLARE
  -- Ambiente / destino (autodetectado por CURRENT_AWS_ACCOUNT)
  v_cuenta_actual VARCHAR(20);
  v_db_actual     VARCHAR(128);
  v_ambiente      VARCHAR(10);
  v_bucket        VARCHAR(500);
  v_iam_role      VARCHAR(500);
  v_base_prefix   VARCHAR(500);   -- prefijo base, p.ej. 'nov'
  v_region        VARCHAR(60);

  -- Contrato de corrida
  v_modo          VARCHAR(64);    -- modo PEDIDO (del mnemotecnico)
  v_modo_efectivo VARCHAR(10);    -- modo APLICADO (FULL si no hay frontera)
  v_bootstrap     BOOLEAN := FALSE;
  v_wm_anterior   TIMESTAMP;      -- frontera: MAX(watermark_nuevo) completado
  v_wm_nuevo      TIMESTAMP;      -- MAX(fecha_actualizacion) del dato de hoy
  v_ts_utc        VARCHAR(32);
  v_run_id        VARCHAR(200);

  -- Control de exportacion (nov_export_control)
  v_firma         VARCHAR(2000);  -- huella de los datos: modo + 7 x (filas:lote:fecha)
  v_parte         VARCHAR(300);
  v_run_prev      VARCHAR(200);   -- run_id 'completado' previo con la misma firma
  v_total         BIGINT  := 0;   -- filas exportadas (suma de las 7 tablas)
  v_registrado    BOOLEAN := FALSE;
  v_run_prefix    VARCHAR(600);   -- <base>/<run_id>
  v_data_prefix   VARCHAR(600);   -- <base>/<run_id>/data

  -- Trabajo por tabla
  v_tabla         VARCHAR(128);
  v_inner         VARCHAR(MAX);   -- SELECT interno del UNLOAD
  v_sql           VARCHAR(MAX);   -- texto del UNLOAD / del CTL
  v_conteo        BIGINT;

  -- Acumulador del arreglo "dominios" del CTL final
  v_dominios      VARCHAR(MAX);
  v_json          VARCHAR(MAX);
  v_ts_iso        VARCHAR(32);
  v_msg           VARCHAR(600);
BEGIN

  v_cuenta_actual := CURRENT_AWS_ACCOUNT;
  v_db_actual     := CURRENT_DATABASE();

  -- ----------------------------------------------------------
  -- 0) Modo FULL/DELTA derivado del mnemotecnico (slot 4). Vacio -> FULL.
  --    DELTA es palabra reservada en Redshift: solo se usa como literal de
  --    cadena ('DELTA'), nunca como identificador.
  -- ----------------------------------------------------------
  v_modo := NULLIF(bdm_datos.fw_resolver_param('MODO', in_nemotecnico), '');
  IF v_modo IS NULL THEN
    v_modo := 'FULL';
  END IF;
  IF v_modo NOT IN ('FULL', 'DELTA') THEN
    v_msg := 'sp_novedades_exportar_7_tablas: modo no soportado (FULL|DELTA): ' || v_modo;
    RAISE EXCEPTION '%', v_msg;
  END IF;

  -- ----------------------------------------------------------
  -- 1) Autodeteccion de ambiente por CURRENT_AWS_ACCOUNT contra nov_config.
  --    (Mismo patron que el Exportador_GEO con geo_config.)
  -- ----------------------------------------------------------
  SELECT ambiente INTO v_ambiente
  FROM bdm_datos.nov_config
  WHERE parametro = 'aws_account_id' AND valor = v_cuenta_actual;

  IF v_ambiente IS NULL THEN
    v_msg := 'sp_novedades_exportar_7_tablas: no se pudo resolver el ambiente para la cuenta AWS '
          || COALESCE(v_cuenta_actual, '(null)') || ' (base ' || COALESCE(v_db_actual, '(null)')
          || '). Verifique bdm_datos.nov_config.aws_account_id.';
    RAISE EXCEPTION '%', v_msg;
  END IF;

  -- Parametros de destino: todos requeridos; abortar identificando el que falte.
  SELECT valor INTO v_bucket
  FROM bdm_datos.nov_config WHERE ambiente = v_ambiente AND parametro = 'triggers_bucket';
  IF v_bucket IS NULL THEN
    v_msg := 'sp_novedades_exportar_7_tablas: parametro ausente: triggers_bucket (ambiente ' || v_ambiente || ').';
    RAISE EXCEPTION '%', v_msg;
  END IF;

  SELECT valor INTO v_iam_role
  FROM bdm_datos.nov_config WHERE ambiente = v_ambiente AND parametro = 'unload_iam_role';
  IF v_iam_role IS NULL THEN
    v_msg := 'sp_novedades_exportar_7_tablas: parametro ausente: unload_iam_role (ambiente ' || v_ambiente || ').';
    RAISE EXCEPTION '%', v_msg;
  END IF;

  SELECT valor INTO v_base_prefix
  FROM bdm_datos.nov_config WHERE ambiente = v_ambiente AND parametro = 's3_base_prefix';
  IF v_base_prefix IS NULL THEN
    v_msg := 'sp_novedades_exportar_7_tablas: parametro ausente: s3_base_prefix (ambiente ' || v_ambiente || ').';
    RAISE EXCEPTION '%', v_msg;
  END IF;

  SELECT valor INTO v_region
  FROM bdm_datos.nov_config WHERE ambiente = v_ambiente AND parametro = 'region';
  IF v_region IS NULL THEN
    v_msg := 'sp_novedades_exportar_7_tablas: parametro ausente: region (ambiente ' || v_ambiente || ').';
    RAISE EXCEPTION '%', v_msg;
  END IF;

  -- ----------------------------------------------------------
  -- 1b) FRONTERA DEL EXPORT Y DECISION DE BOOTSTRAP.
  --   La frontera es SUYA, no la de nov_control: son procesos distintos y el
  --   export puede correr mas veces que la carga. Acotada por ambiente.
  --   Si no hay frontera previa se fuerza FULL aunque se pida DELTA, igual
  --   que 16_sp_unificacion_ciclo.sql:180-185. Sin esto, el primer DELTA tras
  --   un despliegue limpio exportaria CERO filas y el CTL diria que todo esta
  --   al dia.
  -- ----------------------------------------------------------
  SELECT MAX(watermark_nuevo)
    INTO v_wm_anterior
    FROM bdm_datos.nov_export_control
   WHERE estado = 'completado'
     AND ambiente = v_ambiente;

  IF v_wm_anterior IS NULL THEN
    v_modo_efectivo := 'FULL';
    v_bootstrap     := (v_modo = 'DELTA');
  ELSE
    v_modo_efectivo := v_modo;
  END IF;

  -- Frontera nueva: MAX(fecha_actualizacion) del DATO, nunca del reloj. Es la
  -- leccion que ya costo un defecto en sp_novedades_ciclo_edf (watermark que
  -- salia de CURRENT_DATE y dejaba el DELTA ciego).
  SELECT MAX(f) INTO v_wm_nuevo FROM (
    SELECT MAX(fecha_actualizacion) AS f FROM bdm_datos.mat_info_basica
    UNION ALL SELECT MAX(fecha_actualizacion) FROM bdm_datos.mat_info_ubicacion
    UNION ALL SELECT MAX(fecha_actualizacion) FROM bdm_datos.mat_info_actividad_economica
    UNION ALL SELECT MAX(fecha_actualizacion) FROM bdm_datos.mat_ubicacion_agregada
    UNION ALL SELECT MAX(fecha_actualizacion) FROM bdm_datos.mat_info_contactabilidad
    UNION ALL SELECT MAX(fecha_actualizacion) FROM bdm_datos.mat_info_punto_interes
    UNION ALL SELECT MAX(fecha_actualizacion) FROM bdm_datos.mat_info_movilidad
  ) x;
  -- Nunca retroceder: si hoy no hay dato, la frontera se queda donde estaba.
  IF v_wm_nuevo IS NULL OR (v_wm_anterior IS NOT NULL AND v_wm_nuevo < v_wm_anterior) THEN
    v_wm_nuevo := v_wm_anterior;
  END IF;

  -- ----------------------------------------------------------
  -- 2) Firma de los datos y control de re-exportacion (nov_export_control).
  --    Si los datos no cambiaron desde la ultima corrida 'completado' del
  --    ambiente, no se exporta de nuevo (no se crea carpeta en S3).
  -- ----------------------------------------------------------
  v_firma := v_modo_efectivo;

  SELECT 'basica:' || COUNT(*)::VARCHAR || ':' || COALESCE(MAX(lote), 0)::VARCHAR || ':'
         || COALESCE(TO_CHAR(MAX(fecha_actualizacion), 'YYYYMMDDHH24MISS'), '-')
  INTO v_parte FROM bdm_datos.mat_info_basica;
  v_firma := v_firma || '|' || v_parte;

  SELECT 'ubicacion:' || COUNT(*)::VARCHAR || ':' || COALESCE(MAX(lote), 0)::VARCHAR || ':'
         || COALESCE(TO_CHAR(MAX(fecha_actualizacion), 'YYYYMMDDHH24MISS'), '-')
  INTO v_parte FROM bdm_datos.mat_info_ubicacion;
  v_firma := v_firma || '|' || v_parte;

  SELECT 'acteco:' || COUNT(*)::VARCHAR || ':' || COALESCE(MAX(lote), 0)::VARCHAR || ':'
         || COALESCE(TO_CHAR(MAX(fecha_actualizacion), 'YYYYMMDDHH24MISS'), '-')
  INTO v_parte FROM bdm_datos.mat_info_actividad_economica;
  v_firma := v_firma || '|' || v_parte;

  SELECT 'ubi_agregada:' || COUNT(*)::VARCHAR || ':' || COALESCE(MAX(lote), 0)::VARCHAR || ':'
         || COALESCE(TO_CHAR(MAX(fecha_actualizacion), 'YYYYMMDDHH24MISS'), '-')
  INTO v_parte FROM bdm_datos.mat_ubicacion_agregada;
  v_firma := v_firma || '|' || v_parte;

  SELECT 'contact:' || COUNT(*)::VARCHAR || ':' || COALESCE(MAX(lote), 0)::VARCHAR || ':'
         || COALESCE(TO_CHAR(MAX(fecha_actualizacion), 'YYYYMMDDHH24MISS'), '-')
  INTO v_parte FROM bdm_datos.mat_info_contactabilidad;
  v_firma := v_firma || '|' || v_parte;

  SELECT 'pi:' || COUNT(*)::VARCHAR || ':' || COALESCE(MAX(lote), 0)::VARCHAR || ':'
         || COALESCE(TO_CHAR(MAX(fecha_actualizacion), 'YYYYMMDDHH24MISS'), '-')
  INTO v_parte FROM bdm_datos.mat_info_punto_interes;
  v_firma := v_firma || '|' || v_parte;

  SELECT 'movilidad:' || COUNT(*)::VARCHAR || ':' || COALESCE(MAX(lote), 0)::VARCHAR || ':'
         || COALESCE(TO_CHAR(MAX(fecha_actualizacion), 'YYYYMMDDHH24MISS'), '-')
  INTO v_parte FROM bdm_datos.mat_info_movilidad;
  v_firma := v_firma || '|' || v_parte;

  SELECT run_id INTO v_run_prev
  FROM bdm_datos.nov_export_control
  WHERE ambiente = v_ambiente
    AND estado = 'completado'
    AND firma_datos = v_firma
  ORDER BY fecha_fin DESC
  LIMIT 1;

  IF v_run_prev IS NOT NULL THEN
    v_msg := 'sp_novedades_exportar_7_tablas OK sin cambios: los datos ya se exportaron en run_id='
          || v_run_prev || ' (ambiente=' || v_ambiente || '). No se ejecuta UNLOAD.';
    RAISE INFO '%', v_msg;
    RETURN;
  END IF;

  -- ----------------------------------------------------------
  -- 3) run_id UNICO (UTC), prefijos y registro 'en_proceso'.
  -- ----------------------------------------------------------
  v_ts_utc     := TO_CHAR(SYSDATE AT TIME ZONE 'UTC', 'YYYYMMDD"T"HH24MISS"Z"');
  v_run_id     := v_ts_utc || '_' || v_modo_efectivo;
  v_run_prefix := v_base_prefix || '/' || v_run_id;
  v_data_prefix:= v_run_prefix || '/data';
  v_dominios   := '';

  INSERT INTO bdm_datos.nov_export_control (
    run_id, ejecucion_id, ambiente, modo, estado, firma_datos, s3_prefix,
    bootstrap, watermark_anterior, fecha_inicio, usuario_bd)
  VALUES (
    v_run_id, NULLIF(LEFT(TRIM(COALESCE(in_solicitud, '')), 256), ''), v_ambiente, v_modo_efectivo,
    'en_proceso', v_firma, 's3://' || v_bucket || '/' || v_run_prefix || '/',
    v_bootstrap, v_wm_anterior, GETDATE(), CURRENT_USER);
  v_registrado := TRUE;

  -- ==========================================================
  -- 4) UNLOAD de las 7 tablas. Patron por tabla:
  --    a) materializar la salida en bdm_tempo.stg_nov_export_<tabla>
  --    b) contar la staging (SELECT COUNT(*)) -> registros_esperados
  --    c) UNLOAD de la staging a data/<tabla>/ (CSV HEADER GZIP)
  --    d) acumular la entrada del CTL con fn_novedades_dominio_json
  --    El COUNT sale de la staging ANTES del UNLOAD (patron Exportador_GEO).
  -- ==========================================================

  -- d4 mat_info_basica (1 fila por persona) ------------------
  v_tabla := 'mat_info_basica';
  DROP TABLE IF EXISTS bdm_tempo.stg_nov_export_mat_info_basica;
  CREATE TABLE bdm_tempo.stg_nov_export_mat_info_basica AS
  SELECT b.tipo_identificacion || '#' || b.numero_identificacion::VARCHAR AS pk,
         'INFO_BASICA'::VARCHAR AS sk,
         b.*
  FROM bdm_datos.mat_info_basica b
  WHERE b.tipo_identificacion IS NOT NULL AND b.numero_identificacion IS NOT NULL
    AND (v_modo_efectivo = 'FULL' OR b.fecha_actualizacion > v_wm_anterior);
  SELECT COUNT(*) INTO v_conteo FROM bdm_tempo.stg_nov_export_mat_info_basica;
  v_total := v_total + v_conteo;
  CALL bdm_datos.sp_novedades_unload_una(
         'bdm_tempo.stg_nov_export_mat_info_basica', v_bucket, v_data_prefix,
         v_tabla, v_iam_role, v_region);
  v_dominios := v_dominios || bdm_datos.fn_novedades_dominio_json(
         v_tabla, v_bucket, v_data_prefix, v_conteo, (v_dominios <> ''));
  DROP TABLE IF EXISTS bdm_tempo.stg_nov_export_mat_info_basica;

  -- d1 mat_info_ubicacion (N) --------------------------------
  v_tabla := 'mat_info_ubicacion';
  DROP TABLE IF EXISTS bdm_tempo.stg_nov_export_mat_info_ubicacion;
  CREATE TABLE bdm_tempo.stg_nov_export_mat_info_ubicacion AS
  SELECT u.tipo_identificacion || '#' || u.numero_identificacion::VARCHAR AS pk,
         'UBICACION#' || u.id::VARCHAR AS sk,
         u.*
  FROM bdm_datos.mat_info_ubicacion u
  WHERE u.tipo_identificacion IS NOT NULL AND u.numero_identificacion IS NOT NULL
    AND (v_modo_efectivo = 'FULL' OR u.fecha_actualizacion > v_wm_anterior);
  SELECT COUNT(*) INTO v_conteo FROM bdm_tempo.stg_nov_export_mat_info_ubicacion;
  v_total := v_total + v_conteo;
  CALL bdm_datos.sp_novedades_unload_una(
         'bdm_tempo.stg_nov_export_mat_info_ubicacion', v_bucket, v_data_prefix,
         v_tabla, v_iam_role, v_region);
  v_dominios := v_dominios || bdm_datos.fn_novedades_dominio_json(
         v_tabla, v_bucket, v_data_prefix, v_conteo, (v_dominios <> ''));
  DROP TABLE IF EXISTS bdm_tempo.stg_nov_export_mat_info_ubicacion;

  -- d2 mat_info_actividad_economica (N) ----------------------
  v_tabla := 'mat_info_actividad_economica';
  DROP TABLE IF EXISTS bdm_tempo.stg_nov_export_mat_info_acteco;
  CREATE TABLE bdm_tempo.stg_nov_export_mat_info_acteco AS
  SELECT a.tipo_identificacion || '#' || a.numero_identificacion::VARCHAR AS pk,
         'ACTIVIDAD#' || COALESCE(a.ciiu::VARCHAR, '') AS sk,
         a.*
  FROM bdm_datos.mat_info_actividad_economica a
  WHERE a.tipo_identificacion IS NOT NULL AND a.numero_identificacion IS NOT NULL
    AND (v_modo_efectivo = 'FULL' OR a.fecha_actualizacion > v_wm_anterior);
  SELECT COUNT(*) INTO v_conteo FROM bdm_tempo.stg_nov_export_mat_info_acteco;
  v_total := v_total + v_conteo;
  CALL bdm_datos.sp_novedades_unload_una(
         'bdm_tempo.stg_nov_export_mat_info_acteco', v_bucket, v_data_prefix,
         v_tabla, v_iam_role, v_region);
  v_dominios := v_dominios || bdm_datos.fn_novedades_dominio_json(
         v_tabla, v_bucket, v_data_prefix, v_conteo, (v_dominios <> ''));
  DROP TABLE IF EXISTS bdm_tempo.stg_nov_export_mat_info_acteco;

  -- d3 mat_ubicacion_agregada (N) ----------------------------
  --   sk = 7 columnas de la llave; 3 identifican la persona (van en pk) y 4 la
  --   ubicacion (van en sk). ubicacion_dato VA AL FINAL porque las direcciones
  --   pueden traer '#'.
  v_tabla := 'mat_ubicacion_agregada';
  DROP TABLE IF EXISTS bdm_tempo.stg_nov_export_mat_ubi_agregada;
  CREATE TABLE bdm_tempo.stg_nov_export_mat_ubi_agregada AS
  SELECT ag.tipo_identificacion || '#' || ag.numero_identificacion::VARCHAR AS pk,
         'UBIC_AGREGADA#' || COALESCE(ag.ubicacion_codigodane, '') || '#'
           || COALESCE(ag.ubicacion_ciudad, '') || '#'
           || COALESCE(ag.ubicacion_departamento, '') || '#'
           || COALESCE(ag.ubicacion_dato, '') AS sk,
         ag.*
  FROM bdm_datos.mat_ubicacion_agregada ag
  WHERE ag.tipo_identificacion IS NOT NULL AND ag.numero_identificacion IS NOT NULL
    AND (v_modo_efectivo = 'FULL' OR ag.fecha_actualizacion > v_wm_anterior);
  SELECT COUNT(*) INTO v_conteo FROM bdm_tempo.stg_nov_export_mat_ubi_agregada;
  v_total := v_total + v_conteo;
  CALL bdm_datos.sp_novedades_unload_una(
         'bdm_tempo.stg_nov_export_mat_ubi_agregada', v_bucket, v_data_prefix,
         v_tabla, v_iam_role, v_region);
  v_dominios := v_dominios || bdm_datos.fn_novedades_dominio_json(
         v_tabla, v_bucket, v_data_prefix, v_conteo, (v_dominios <> ''));
  DROP TABLE IF EXISTS bdm_tempo.stg_nov_export_mat_ubi_agregada;

  -- d5 mat_info_contactabilidad (N, cruce 1 a 1 con d1) ------
  --   tipo/numero vienen del cruce con d1 (van tras el sk). De su llave
  --   (id, sector, ubicacion_id) solo se usa ubicacion_id.
  v_tabla := 'mat_info_contactabilidad';
  DROP TABLE IF EXISTS bdm_tempo.stg_nov_export_mat_info_contact;
  CREATE TABLE bdm_tempo.stg_nov_export_mat_info_contact AS
  SELECT u.tipo_identificacion || '#' || u.numero_identificacion::VARCHAR AS pk,
         'CONTACTABILIDAD#' || c.ubicacion_id::VARCHAR AS sk,
         u.tipo_identificacion,
         u.numero_identificacion,
         c.*
  FROM bdm_datos.mat_info_contactabilidad c
  JOIN bdm_datos.mat_info_ubicacion u ON u.id = c.ubicacion_id
  WHERE u.tipo_identificacion IS NOT NULL AND u.numero_identificacion IS NOT NULL
    AND (v_modo_efectivo = 'FULL' OR c.fecha_actualizacion > v_wm_anterior);
  SELECT COUNT(*) INTO v_conteo FROM bdm_tempo.stg_nov_export_mat_info_contact;
  v_total := v_total + v_conteo;
  CALL bdm_datos.sp_novedades_unload_una(
         'bdm_tempo.stg_nov_export_mat_info_contact', v_bucket, v_data_prefix,
         v_tabla, v_iam_role, v_region);
  v_dominios := v_dominios || bdm_datos.fn_novedades_dominio_json(
         v_tabla, v_bucket, v_data_prefix, v_conteo, (v_dominios <> ''));
  DROP TABLE IF EXISTS bdm_tempo.stg_nov_export_mat_info_contact;

  -- d7 mat_info_punto_interes (N, duplicado por persona) -----
  --   la ubicacion fisica puede ser de varias personas: DISTINCT para no
  --   repetir la misma persona (decision 2026-10-22). Cruce por cod_dw_ubic.
  v_tabla := 'mat_info_punto_interes';
  DROP TABLE IF EXISTS bdm_tempo.stg_nov_export_mat_info_pi;
  CREATE TABLE bdm_tempo.stg_nov_export_mat_info_pi AS
  SELECT DISTINCT
         u.tipo_identificacion || '#' || u.numero_identificacion::VARCHAR AS pk,
         'PUNTO_INTERES#' || p.ubicacion_id::VARCHAR || '#' || p.id::VARCHAR AS sk,
         u.tipo_identificacion,
         u.numero_identificacion,
         p.*
  FROM bdm_datos.mat_info_punto_interes p
  JOIN bdm_datos.mat_info_ubicacion u ON u.cod_dw_ubic = p.ubicacion_id
  WHERE u.tipo_identificacion IS NOT NULL AND u.numero_identificacion IS NOT NULL
    AND (v_modo_efectivo = 'FULL' OR p.fecha_actualizacion > v_wm_anterior);
  SELECT COUNT(*) INTO v_conteo FROM bdm_tempo.stg_nov_export_mat_info_pi;
  v_total := v_total + v_conteo;
  CALL bdm_datos.sp_novedades_unload_una(
         'bdm_tempo.stg_nov_export_mat_info_pi', v_bucket, v_data_prefix,
         v_tabla, v_iam_role, v_region);
  v_dominios := v_dominios || bdm_datos.fn_novedades_dominio_json(
         v_tabla, v_bucket, v_data_prefix, v_conteo, (v_dominios <> ''));
  DROP TABLE IF EXISTS bdm_tempo.stg_nov_export_mat_info_pi;

  -- d6 mat_info_movilidad (0..N) -----------------------------
  v_tabla := 'mat_info_movilidad';
  DROP TABLE IF EXISTS bdm_tempo.stg_nov_export_mat_info_movilidad;
  CREATE TABLE bdm_tempo.stg_nov_export_mat_info_movilidad AS
  SELECT m.tipo_identificacion || '#' || m.numero_identificacion::VARCHAR AS pk,
         'MOVILIDAD#' || m.id::VARCHAR AS sk,
         m.*
  FROM bdm_datos.mat_info_movilidad m
  WHERE m.tipo_identificacion IS NOT NULL AND m.numero_identificacion IS NOT NULL;
  SELECT COUNT(*) INTO v_conteo FROM bdm_tempo.stg_nov_export_mat_info_movilidad;
  v_total := v_total + v_conteo;
  CALL bdm_datos.sp_novedades_unload_una(
         'bdm_tempo.stg_nov_export_mat_info_movilidad', v_bucket, v_data_prefix,
         v_tabla, v_iam_role, v_region);
  v_dominios := v_dominios || bdm_datos.fn_novedades_dominio_json(
         v_tabla, v_bucket, v_data_prefix, v_conteo, (v_dominios <> ''));
  DROP TABLE IF EXISTS bdm_tempo.stg_nov_export_mat_info_movilidad;

  -- ==========================================================
  -- 5) CTL final. Se escribe SOLO ahora que las 7 terminaron (GUIA): los dos
  --    consumidores (DynamoDB y Bureau) arrancan con este archivo. UNLOAD de
  --    una sola fila, PARALLEL OFF, EXTENSION 'json', sin CSV ni GZIP. OJO
  --    (GUIA seccion 4): Redshift agrega 000 al nombre (7_tablas_finish000.json);
  --    la regla de EventBridge debe aceptarlo.
  -- ==========================================================
  v_ts_iso := TO_CHAR(SYSDATE AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"');
  v_json :=
       '{'
    || '"run_id":"'     || v_run_id     || '",'
    || '"tipo_carga":"' || v_modo_efectivo || '",'
    || '"fecha_fin":"'  || v_ts_iso     || '",'
    || '"dominios":['   || v_dominios   || ']'
    || '}';

  -- El JSON va como LITERAL de cadena dentro del SELECT del UNLOAD:
  --   UNLOAD ('SELECT ''{...}''') ...
  -- (comilla doble-escapada dentro del texto del UNLOAD). Sin las comillas el
  -- SELECT interno seria "SELECT {...}" y fallaria por sintaxis. v_json se arma
  -- solo con valores controlados (run_id, modo, fecha, nombres de tabla, rutas
  -- S3); por seguridad se eliminan comillas simples.
  v_json := REPLACE(v_json, '''', '');
  v_sql :=
       'UNLOAD (''SELECT ''''' || v_json || ''''''') '
    || 'TO ''s3://' || v_bucket || '/' || v_run_prefix || '/7_tablas_finish'' '
    || 'IAM_ROLE ''' || v_iam_role || ''' '
    || 'PARALLEL OFF EXTENSION ''json'' ALLOWOVERWRITE '
    || 'REGION ''' || v_region || '''';
  EXECUTE v_sql;

  UPDATE bdm_datos.nov_export_control
  SET estado          = 'completado',
      registros_total = v_total,
      watermark_nuevo = v_wm_nuevo,
      fecha_fin       = GETDATE()
  WHERE run_id = v_run_id;

  v_msg := 'sp_novedades_exportar_7_tablas OK run_id=' || v_run_id || ' ambiente=' || v_ambiente;
  RAISE INFO '%', v_msg;

EXCEPTION WHEN OTHERS THEN
  -- Marcar la corrida como fallida (no bloquea la siguiente) y propagar.
  IF v_registrado THEN
    UPDATE bdm_datos.nov_export_control
    SET estado       = 'fallido',
        ultimo_error = LEFT(SQLERRM, 4000),
        fecha_fin    = GETDATE()
    WHERE run_id = v_run_id;
  END IF;
  RAISE EXCEPTION 'sp_novedades_exportar_7_tablas failed: %', SQLERRM;
END;
$$;
