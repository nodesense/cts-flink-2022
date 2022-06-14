
open terminal 

````
wget -P $FLINK_HOME/lib https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-2.3.6_2.12/1.15.0/flink-sql-connector-hive-2.3.6_2.12-1.15.0.jar
```

```
$FLINK_HOME/bin/stop-cluster.sh
$FLINK_HOME/bin/start-cluster.sh

```

open sql client 

```
$FLINK_HOME/bin/sql-client.sh
```


open sql client


```
CREATE CATALOG myhive WITH (
  'type' = 'hive',
  'hive-conf-dir' = '/opt/apache-hive-2.3.6/conf'
);

 
use catalog myhive;
```

```
SHOW DATABASES
```

```
SHOW TABLES
```

```
SELECT *  FROM myhive.moviedb.movies m INNER JOIN myhive.moviedb.ratings r ON r.movie_id = m.movie_id ;


```


```sql
SELECT m.movie_id, avg(rating) as avg_rating, sum(user_id) as total_ratings FROM myhive.moviedb.movies m INNER JOIN myhive.moviedb.ratings r ON r.movie_id = m.movie_id group by m.movie_id
;
```



```sql
SELECT m.movie_id, avg(rating) as avg_rating, sum(user_id) as total_ratings FROM myhive.moviedb.movies m INNER JOIN myhive.moviedb.ratings r ON r.movie_id = m.movie_id group by m.movie_id

HAVING avg(rating) >= 4 AND sum(user_id) >= 100
;
```
