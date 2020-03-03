create table building_use_generic_reclass_2014 select * from psrc_2014_parcel_baseyear_working.building_use_generic_reclass;

select county_id, building_type_id, use_code, count(*) from buildings_20200212_pmc group by county_id, building_type_id, use_code;
# set index on the reclass table
alter table building_use_generic_reclass_2018_v1 add index (county_id,  county_building_use_code);

# add new columns to be updated
alter table buildings_20200219_hs add column new_building_type_id int;
alter table buildings_20200219_hs add column new_use_code varchar(50);
alter table buildings_20200219_hs add column use_code_orig varchar(50);
update buildings_20200219_hs set new_use_code = use_code;
update buildings_20200219_hs set new_building_type_id = 0;

# set index for faster join
alter table buildings_20200219_hs add index (building_id, county_id,  new_use_code);

# join
update buildings_20200219_hs as a, building_use_generic_reclass_2018_v1 as b set a.new_building_type_id = b.building_type_id where a.county_id = b.county_id and a.new_use_code = b.county_building_use_code;

# check missed cases
select count(*) from buildings_20200219_hs where new_building_type_id = 0;
select county_id, new_building_type_id, new_use_code, count(*) from buildings_20200219_hs group by county_id, new_building_type_id, use_code;
select county_id, new_building_type_id, new_use_code, count(*) from buildings_20200219_hs where new_building_type_id = 0 group by county_id, new_building_type_id, use_code;

# fixing some use codes
update buildings_20200219_hs set new_use_code = "mobile home" where use_code = "mobile_home" and county_id = 35;
update buildings_20200219_hs set new_use_code = "Apartment High Rise Shell" where use_code = "Apartment, High Rise, Shell" and county_id = 35;
update buildings_20200219_hs set new_use_code = "5" where use_code = "Multi-family" and county_id = 35;
update buildings_20200219_hs set new_use_code = "Post Office Branch" where use_code = "Post Office, Branch" and county_id = 35;
update buildings_20200219_hs set new_use_code = "Shell Office Building" where use_code = "Shell, Office Building" and county_id = 35;
update buildings_20200219_hs set new_use_code = "Car Wash Drive-Thru" where use_code = "Car Wash, Drive-Thru" and county_id = 35;
update buildings_20200219_hs set new_use_code = "Car Wash Self-Serve" where use_code = "Car Wash, Self-Serve" and county_id = 35;
update buildings_20200219_hs set new_use_code = "Motel Room 2 Sty-Single Row" where use_code = "Motel Room, 2 Sty-Single Row" and county_id = 35;
update buildings_20200219_hs set new_use_code = "Motel Room 1 Sty-Single Row" where use_code = "Motel Room, 1 Sty-Single Row" and county_id = 35;
update buildings_20200219_hs set new_use_code = "APART" where use_code = "APARTHRS" and county_id = 61;
update buildings_20200219_hs set new_use_code = "MOTEL1SR" where use_code = "MOTELES" and county_id = 61;
update buildings_20200219_hs set new_use_code = "SERVICE" where use_code = "SERVICEB" and county_id = 61;
update buildings_20200219_hs set new_use_code = "MULTRESR" where use_code = "MULTRESA" and county_id = 61;

# now repeat the join above and recheck missed cases
# if OK, store into the right columns
update buildings_20200219_hs set building_type_id = new_building_type_id;
update buildings_20200219_hs set use_code_orig = use_code;
update buildings_20200219_hs set use_code = new_use_code;

# delete temp columns
alter table buildings_20200219_hs drop column new_building_type_id;
alter table buildings_20200219_hs drop column new_use_code;

# misc
select * from buildings_20200219_hs where use_code = "Multi-family";

select county_id, building_type_id, use_code, count(*) from buildings_20200219_hs where building_type_id = 0 group by county_id, building_type_id, use_code;

select * from building_use_generic_reclass_2018_v1 where county_id = 35 and county_building_use_code like "Motel%";