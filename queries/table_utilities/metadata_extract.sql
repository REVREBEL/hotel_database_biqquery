SELECT 
  table_schema, 
  table_name, 
  column_name, 
  description -- This identifies which ones have meta-descriptions
FROM 
  `devrebel-big-query-database`.`region-us-central1`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
WHERE 
  table_schema IN ('dev_hotel_analytics', 'dev_hotel_costar', 'dev_hotel_forecast', 'dev_hotel_g4a', 'dev_hotel_sales', 'dev_hotel_settings', 'devrebel')
ORDER BY 
  column_name;