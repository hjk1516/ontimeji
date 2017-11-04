drop table ontime_ji;

create table ontime_ji
(
   Year                int,
   UniqueCarrier       varchar(10),
   val					  int
);

select year, max(val)
  from ontime_ji
 group by year;
  
 
 
select UniqueCarrier, Description
  from ontime_ji, carrier
 where ontime_ji.UniqueCarrier = carrier.Description;
  
select e.UniqueCarrier, d.Description
  from ontime_ji e, carrier d
where e.uniquecarrier = d.code;  


-- insert into finalji (Year, UniqueCarrier, val, description) 

select e.Year, e.UniqueCarrier, max(e.val), c.description
 from ontime_ji e, carrier c
where e.uniquecarrier = c.code
 group by e.Year;
 
select Year, max(val)
 from finalji
group by Year, uniquecarrier; 

create table finalji
(
   Year                int,
   UniqueCarrier       varchar(10),
   val					  int,
   description		  varchar(100)
);
 
 select e.Year, c.code, max(e.val), c.description
 from ontime_ji e, carrier c
where e.uniquecarrier = c.code
 group by e.Year;
 
select e.year, e.val, e.uniquecarrier, d.description
  from ontime_ji e INNER JOIN  carrier d
	on e.UniqueCarrier = d.code
 where val IN (select max(val)
  				   from ontime_ji 
				  group by year );
