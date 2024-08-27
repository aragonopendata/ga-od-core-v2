set -e
export PGPASSWORD=$POSTGRES_PASSWORD
psql -h $POSTGRESQL_HOST -p $POSTGRESQL_PORT -d $POSTGRES_DB  -U $POSTGRES_USER  -c "
			drop materialized view if exists public.v_resources_count;
			drop materialized view if exists public.v_ip_count;
			drop materialized view if exists public.v_resources_ip;
                        drop materialized view if exists public.v_requests_gaodcore;
            CREATE materialized VIEW public.v_requests_gaodcore as SELECT TO_CHAR(ear.datetime, 'YYYY-MM-DD HH:MM:SS') as datetime, url,
				 case
				 	when query_string!='' and query_string ~ 'resource_id=[0-9]*&' and (url = '/GA_OD_Core/preview' or url = '/GA_OD_Core/download')
				 		then
				 			(select name from gaodcore_manager_resourceconfig gmr2  where id  = SUBSTRING((regexp_match(query_string,'resource.*'))[1], Position('resource_id=' in (regexp_match(query_string,'resource.*'))[1]) + LENGTH('resource_id='),
                 			 Position('&' in (regexp_match(query_string,'resource.*'))[1]) - Position('resource_id=' in (regexp_match(query_string,'resource.*'))[1]) - LENGTH('resource_id='))::Integer)
					when query_string!='' and query_string ~ 'resource_id=[0-9]*' and  query_string not LIKE '%resource_id=%&%' and (url = '/GA_OD_Core/preview' or url = '/GA_OD_Core/download')

						then
							(select name from gaodcore_manager_resourceconfig gmr2  where id  = SUBSTRING((regexp_match(query_string,'resource.*'))[1], Position('resource_id=' in (regexp_match(query_string,'resource.*'))[1]) + LENGTH('resource_id='),
                 			 LENGTH((regexp_match(query_string,'resource.*'))[1]))::Integer)
                 	when query_string!='' and query_string ~ 'view_id=[0-9]*&' and (url = '/GA_OD_Core/preview' or url = '/GA_OD_Core/download')

				 		then
				 			(select name from gaodcore_manager_resourceconfig gmr2  where id  = SUBSTRING( (regexp_match(query_string,'view_id.*'))[1], Position('view_id=' in  (regexp_match(query_string,'view_id.*'))[1]) + LENGTH('view_id='),
                 			 Position('&' in  (regexp_match(query_string,'view_id.*'))[1]) - Position('view_id=' in  (regexp_match(query_string,'view_id.*'))[1]) - LENGTH('view_id='))::Integer)
					when query_string!='' and query_string ~ 'view_id=[0-9]*' and  query_string not LIKE '%view_id=%&%' and (url = '/GA_OD_Core/preview' or url = '/GA_OD_Core/download')

						then
							(select name from gaodcore_manager_resourceconfig gmr2  where id  = SUBSTRING( (regexp_match(query_string,'view_id.*'))[1], Position('view_id=' in  (regexp_match(query_string,'view_id.*'))[1]) + LENGTH('view_id='),
                 			 LENGTH( (regexp_match(query_string,'view_id.*'))[1]))::Integer)
				 end
				 as resource_name,
                 query_string,
                 user_id,
                 remote_ip,
                 case
    				when ear.remote_ip <> ''::text and ear.remote_ip LIKE '%,%'::text
    			then split_part(remote_ip,',',1)
    			else 
    				remote_ip
    			 end as ip
                 from easyaudit_requestevent ear;
		CREATE materialized VIEW public.v_resources_count as select resource_name, count(resource_name)  from v_requests_gaodcore vrg group by resource_name;
 CREATE materialized VIEW public.v_ip_count as select ip, count(ip) from v_requests_gaodcore vrg  group by ip  ;
 create materialized view public.v_resources_ip as select resource_name, count(distinct(ip)) as numero_llamadas from v_requests_gaodcore vrg group by resource_name;

GRANT SELECT on v_resources_ip TO gaodcore_read;
GRANT SELECT on v_requests_gaodcore TO gaodcore_read;
GRANT SELECT on v_ip_count TO gaodcore_read;
GRANT SELECT on v_resources_count TO gaodcore_read; 

"
