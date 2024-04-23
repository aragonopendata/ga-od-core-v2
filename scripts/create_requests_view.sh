set -e
export PGPASSWORD=$POSTGRES_PASSWORD
psql -h postgres -p 5432 -d $POSTGRES_DB  -U $POSTGRES_USER  -c "create OR REPLACE view public.v_requests_gaodcore as SELECT datetime, url,
				 case
				 	when query_string!='' and query_string LIKE '%resource_id=%&%' and (url = '/GA_OD_Core/preview' or url = '/GA_OD_Core/download')
				 		then
				 			(select name from gaodcore_manager_resourceconfig gmr2  where id  = SUBSTRING((regexp_match(query_string,'resource.*'))[1], Position('resource_id=' in (regexp_match(query_string,'resource.*'))[1]) + LENGTH('resource_id='),
                 			 Position('&' in (regexp_match(query_string,'resource.*'))[1]) - Position('resource_id=' in (regexp_match(query_string,'resource.*'))[1]) - LENGTH('resource_id='))::Integer)
					when query_string!='' and query_string LIKE '%resource_id=%' and  query_string not LIKE '%resource_id=%&%' and (url = '/GA_OD_Core/preview' or url = '/GA_OD_Core/download')

						then
							(select name from gaodcore_manager_resourceconfig gmr2  where id  = SUBSTRING((regexp_match(query_string,'resource.*'))[1], Position('resource_id=' in (regexp_match(query_string,'resource.*'))[1]) + LENGTH('resource_id='),
                 			 LENGTH((regexp_match(query_string,'resource.*'))[1]))::Integer)
                 	when query_string!='' and query_string LIKE '%view_id=%&%' and (url = '/GA_OD_Core/preview' or url = '/GA_OD_Core/download')

				 		then
				 			(select name from gaodcore_manager_resourceconfig gmr2  where id  = SUBSTRING( (regexp_match(query_string,'view_id.*'))[1], Position('view_id=' in  (regexp_match(query_string,'view_id.*'))[1]) + LENGTH('view_id='),
                 			 Position('&' in  (regexp_match(query_string,'view_id.*'))[1]) - Position('view_id=' in  (regexp_match(query_string,'view_id.*'))[1]) - LENGTH('view_id='))::Integer)
					when query_string!='' and query_string LIKE '%view_id=%' and  query_string not LIKE '%view_id=%&%' and (url = '/GA_OD_Core/preview' or url = '/GA_OD_Core/download')

						then
							(select name from gaodcore_manager_resourceconfig gmr2  where id  = SUBSTRING( (regexp_match(query_string,'view_id.*'))[1], Position('view_id=' in  (regexp_match(query_string,'view_id.*'))[1]) + LENGTH('view_id='),
                 			 LENGTH( (regexp_match(query_string,'view_id.*'))[1]))::Integer)
				 end
				 as resource_name,
                 query_string,
                 user_id,
                 remote_ip 
                 from easyaudit_requestevent ear;
"