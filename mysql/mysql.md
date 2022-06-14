```
sudo mysql -u root


SELECT User,Host FROM mysql.user;

CREATE USER 'team'@'localhost' IDENTIFIED BY 'team1234';

CREATE DATABASE ecommerce; 

GRANT ALL PRIVILEGES ON ecommerce.* TO 'team'@'localhost';

exit
```

```
mysql -uteam -pteam1234
```


```
USE ecommerce;

-- detect insert/update changes using timestamp , but HARD delete

create table products (id int, 
                       name varchar(255), 
                       price int);
                       


 
insert into products (id, name,price) values(1, 'product1', 100);
insert into products (id, name,price) values(2, 'product2', 200);
insert into products (id, name,price) values(3, 'product3', 300);
insert into products (id, name,price) values(4, 'product4', 400);




CREATE TABLE person2 (     PersonID int,     Name varchar(255) );
CREATE TABLE person (     PersonID int,     Name varchar(255) );


insert into person values (1, 'joe');
insert into person values (1, 'james');

select * from person ;


```







