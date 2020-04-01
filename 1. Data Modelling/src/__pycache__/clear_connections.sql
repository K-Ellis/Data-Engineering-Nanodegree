REVOKE CONNECT ON DATABASE sparkifydb FROM PUBLIC;

SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM  pg_stat_activity
WHERE
  	pg_stat_activity.datname = 'sparkifydb'
	--AND pid <> pg_backend_pid();  -- don't kill my own connection!
