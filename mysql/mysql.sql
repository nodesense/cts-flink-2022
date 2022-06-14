SET sql-client.execution.result-mode=TABLEAU;

CREATE CATALOG my_catalog WITH(
    'type' = 'jdbc',
    'default-database' = 'ecommerce',
    'username' = 'team',
    'password' = 'team1234',
    'base-url' = 'jdbc:mysql://localhost:3306'
);

USE CATALOG my_catalog;

SHOW CATALOGS;

SHOW DATABASES;

SHOW TABLES;

SELECT * FROM person;

INSERT INTO person2 SELECT * FROM person;
