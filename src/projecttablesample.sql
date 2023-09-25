use ssb;
create table postalcode(postalcode varchar(50) not null primary key,population int ,Area int );
create table hubRepair(HubId varchar(50) not null,employeeId varchar(50), repairTime float, inService boolean);
create table hubDamage(HubId varchar(50) not null primary key ,repairEstimate float);
create table distribHub(HubId varchar(50) not null ,Location varchar(100) ,ServicedArea varchar(50));
create table postalBypopulation(postalcode varchar(50) not null primary key,hubsbypopulation float);
create table postalByarea(postalcode varchar(50) not null primary key,hubsbyarea float);

alter table distribHub add column population float;
alter table hubDamage add column totaltime float;
alter table postalcode add column repairTime float,add Hubcnt int,add hubsCovered varchar(100);

select * from postalcode;
select * from distribHub;
select * from hubDamage;
select * from hubRepair;

