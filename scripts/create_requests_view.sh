set -e
echo "Creating views for GA_OD_Core requests..."
export PGPASSWORD=$POSTGRES_PASSWORD
psql -h $POSTGRESQL_HOST -p $POSTGRESQL_PORT -d $POSTGRES_DB  -U $POSTGRES_USER  -c "
			drop materialized view if exists public.v_resources_count;
			drop materialized view if exists public.v_ip_count;
			drop materialized view if exists public.v_resources_ip;
                        drop materialized view if exists public.v_requests_gaodcore;
                        drop materialized view if exists public.v_requests_gaodcore_t;
                        drop materialized view if exists public.v_id_resourcename;
                        alter table easyaudit_requestevent ALTER column remote_ip TYPE  VARCHAR (100);

			CREATE MATERIALIZED VIEW public.v_requests_gaodcore
			TABLESPACE pg_default
			AS SELECT to_char(ear.datetime, 'YYYY-MM-DD HH24:MI:SS'::text) AS datetime,
				ear.url,
					CASE
						WHEN ear.query_string <> ''::text AND ear.query_string ~ 'resource_id=[0-9]+(&|$)'::text AND (ear.url::text = '/GA_OD_Core/preview'::text OR ear.url::text = '/GA_OD_Core/download'::text) THEN ( SELECT gmr2.name
						FROM gaodcore_manager_resourceconfig gmr2
						WHERE gmr2.id = (regexp_match(ear.query_string, 'resource_id=([0-9]+)(&|$)'::text))[1]::integer)
						WHEN ear.query_string <> ''::text AND ear.query_string ~ 'resource_id=[0-9]+($|&)'::text AND ear.query_string !~ '%resource_id=%&%'::text AND (ear.url::text = '/GA_OD_Core/preview'::text OR ear.url::text = '/GA_OD_Core/download'::text) THEN ( SELECT gmr2.name
						FROM gaodcore_manager_resourceconfig gmr2
						WHERE gmr2.id = (regexp_match(ear.query_string, 'resource_id=([0-9]+)($|&)'::text))[1]::integer)
						WHEN ear.query_string <> ''::text AND ear.query_string ~ 'view_id=[0-9]+(&|$)'::text AND (ear.url::text = '/GA_OD_Core/preview'::text OR ear.url::text = '/GA_OD_Core/download'::text) THEN ( SELECT gmr2.name
						FROM gaodcore_manager_resourceconfig gmr2
						WHERE gmr2.id = (regexp_match(ear.query_string, 'view_id=([0-9]+)(&|$)'::text))[1]::integer)
						WHEN ear.query_string <> ''::text AND ear.query_string ~ 'view_id=[0-9]+($|&)'::text AND ear.query_string !~ '%view_id=%&%'::text AND (ear.url::text = '/GA_OD_Core/preview'::text OR ear.url::text = '/GA_OD_Core/download'::text) THEN ( SELECT gmr2.name
						FROM gaodcore_manager_resourceconfig gmr2
						WHERE gmr2.id = (regexp_match(ear.query_string, 'view_id=([0-9]+)($|&)'::text))[1]::integer)
						ELSE NULL::character varying
					END AS resource_name,
				ear.query_string,
				ear.user_id,
				ear.remote_ip,
					CASE
						WHEN ear.remote_ip::text <> ''::text AND ear.remote_ip::text ~~ '%,%'::text THEN split_part(ear.remote_ip::text, ','::text, 1)::character varying
						ELSE ear.remote_ip
					END AS ip
			FROM easyaudit_requestevent ear
			WITH DATA;

CREATE materialized VIEW public.v_resources_count as select resource_name, count(resource_name)  from v_requests_gaodcore vrg group by resource_name;
CREATE materialized VIEW public.v_ip_count as select ip, count(ip) from v_requests_gaodcore vrg  group by ip  ;
create materialized view public.v_resources_ip as select resource_name, count(distinct(ip)) as numero_llamadas from v_requests_gaodcore vrg group by resource_name;

GRANT SELECT on v_resources_ip TO gaodcore_read;
GRANT SELECT on v_requests_gaodcore TO gaodcore_read;
GRANT SELECT on v_ip_count TO gaodcore_read;
GRANT SELECT on v_resources_count TO gaodcore_read;


-- public.v_id_resourcename source

CREATE MATERIALIZED VIEW public.v_id_resourcename
TABLESPACE pg_default
AS SELECT gaodcore_manager_resourceconfig.id,
    gaodcore_manager_resourceconfig.name
   FROM gaodcore_manager_resourceconfig
WITH DATA;


-- public.v_requests_gaodcore_t source

CREATE MATERIALIZED VIEW public.v_requests_gaodcore_t
TABLESPACE pg_default
AS SELECT to_char(ear.datetime, 'YYYY-MM-DD HH24:MI:SS'::text) AS datetime,
    ear.url,
        CASE
            WHEN ear.query_string <> ''::text AND ear.query_string ~ 'resource_id=[0-9]+(&|$)'::text AND (ear.url::text = '/GA_OD_Core/preview'::text OR ear.url::text = '/GA_OD_Core/download'::text) THEN ( SELECT gmr2.name
               FROM gaodcore_manager_resourceconfig gmr2
              WHERE gmr2.id = (regexp_match(ear.query_string, 'resource_id=([0-9]+)(&|$)'::text))[1]::integer)
            WHEN ear.query_string <> ''::text AND ear.query_string ~ 'resource_id=[0-9]+($|&)'::text AND ear.query_string !~ '%resource_id=%&%'::text AND (ear.url::text = '/GA_OD_Core/preview'::text OR ear.url::text = '/GA_OD_Core/download'::text) THEN ( SELECT gmr2.name
               FROM gaodcore_manager_resourceconfig gmr2
              WHERE gmr2.id = (regexp_match(ear.query_string, 'resource_id=([0-9]+)($|&)'::text))[1]::integer)
            WHEN ear.query_string <> ''::text AND ear.query_string ~ 'view_id=[0-9]+(&|$)'::text AND (ear.url::text = '/GA_OD_Core/preview'::text OR ear.url::text = '/GA_OD_Core/download'::text) THEN ( SELECT gmr2.name
               FROM gaodcore_manager_resourceconfig gmr2
              WHERE gmr2.id = (regexp_match(ear.query_string, 'view_id=([0-9]+)(&|$)'::text))[1]::integer)
            WHEN ear.query_string <> ''::text AND ear.query_string ~ 'view_id=[0-9]+($|&)'::text AND ear.query_string !~ '%view_id=%&%'::text AND (ear.url::text = '/GA_OD_Core/preview'::text OR ear.url::text = '/GA_OD_Core/download'::text) THEN ( SELECT gmr2.name
               FROM gaodcore_manager_resourceconfig gmr2
              WHERE gmr2.id = (regexp_match(ear.query_string, 'view_id=([0-9]+)($|&)'::text))[1]::integer)
            ELSE NULL::character varying
        END AS resource_name,
    ear.query_string,
    ear.user_id,
    ear.remote_ip,
        CASE
            WHEN ear.remote_ip::text <> ''::text AND ear.remote_ip::text ~~ '%,%'::text THEN split_part(ear.remote_ip::text, ','::text, 1)::character varying
            ELSE ear.remote_ip
        END AS ip
   FROM easyaudit_requestevent ear
WITH DATA;

GRANT SELECT on v_id_resourcename TO gaodcore_read;
GRANT SELECT on v_requests_gaodcore_t TO gaodcore_read;

"
