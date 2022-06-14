CREATE CATALOG myhive WITH (
  'type' = 'hive',
  'hive-conf-dir' = '/opt/apache-hive-2.3.6/conf'
);

 
use catalog myhive;
 