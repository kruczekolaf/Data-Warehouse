USE master;
go

-- Datenbank erstellen  
create database DataWarehouse;
go
  
use DataWarehouse;

-- Schemen erstellen für jeden Layer
create Schema bronze;
go
create Schema silver;
go
create Schema gold;
go
