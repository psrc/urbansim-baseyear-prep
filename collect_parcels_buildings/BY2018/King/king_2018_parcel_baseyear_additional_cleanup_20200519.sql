
-- Make a copy of king_2018_parcel_baseyear.buildings with today's date
drop table if exists buildings_20200519;

create table buildings_20200519
select * from buildings_20200209;

alter table buildings_20200519 add index building_id_index(building_id), add index parcel_id_index(parcel_id), add index use_code_index(use_code);

alter table buildings_20200519 add column res_units_imputed int(5);
update buildings_20200519 set res_units_imputed = 0;

-- create a temp table with all the residential use codes having no units

drop table if exists temp_king_res_codes_no_units;

create temporary table temp_king_res_codes_no_units
SELECT
parcel_id
,building_id
,left(parcel_id,6) as major
,right(parcel_id,4) as minor
,residential_units
,improvement_value
,gross_sqft
,non_residential_sqft
,stories
,use_code
from buildings_20200519
where residential_units = 0
and use_code in (1,2,3,4,300,338,348,351,352); -- removed code 800, as SQFT per unit so far = 0

alter table temp_king_res_codes_no_units add index building_id_index(building_id), add index parcel_id_index(parcel_id), add index use_code_index(use_code);

-- create a temp table with current SQFT per unit values for all res use codes that do have units

drop table if exists temp_king_sqft_per_unit_res_codes;

create temporary table temp_king_sqft_per_unit_res_codes
select
use_code
,count(*) as records
,sum(residential_units) as du
,sum(gross_sqft) as gross_sqft
,sum(non_residential_sqft) as nonres_sqft
,round(sum(gross_sqft)/sum(residential_units),1) as sqft_per_unit
from buildings_20200519
where use_code in (1,2,3,4,300,338,348,351,352,800) and residential_units > 0
group by
use_code;

alter table temp_king_sqft_per_unit_res_codes add index use_code_index(use_code);

-- use current SQFT per unit values to imput units where missing

update temp_king_res_codes_no_units a
inner join temp_king_sqft_per_unit_res_codes b
on a.use_code = b.use_code
set a.residential_units = round(a.gross_sqft / b.sqft_per_unit,0);

update temp_king_res_codes_no_units a
set residential_units = 1 where residential_units = 0;

update buildings_20200519 a
inner join temp_king_res_codes_no_units b
on a.building_id = b.building_id
set a.residential_units = b.residential_units
,a.res_units_imputed = 1;

-- fix hospitals and group quarters res units

drop table if exists temp_king_gq_hosp_use_codes;

create temporary table temp_king_gq_hosp_use_codes
SELECT
county_building_use_code as use_code
,county_building_use_description
,building_type_id
,building_type_name
FROM 2018_parcel_baseyear_working.building_use_generic_reclass_2018_v2 where county_id = 33 and building_type_id in (6,7);

/*  -- Investigative work
select
a.use_code
,b.county_building_use_description
,count(*) as records
,sum(residential_units) as du
,sum(gross_sqft) as gross_sqft
,sum(non_residential_sqft) as nonres_sqft
from buildings_20200512 a
inner join temp_king_gq_hosp_use_codes b
on a.use_code = b.use_code
group by
a.use_code
,b.county_building_use_description;
*/

-- Decided after reviewing data to zero out all residential unit fields for these use codes - same as what Hana does with a part of the imputation script

update buildings_20200519 a
inner join temp_king_gq_hosp_use_codes b
on a.use_code = b.use_code
set a.residential_units = 0;

-- select sum(residential_units) from buildings_20200519;

-- investigate Mixed Use codes

/*
drop table if exists temp_king_split_use_codes;

create temporary table temp_king_split_use_codes
SELECT
county_building_use_code as use_code
,county_building_use_description
,building_type_id
,building_type_name
FROM 2018_parcel_baseyear_working.building_use_generic_reclass_2018_v2 where county_id = 33 and building_type_id in (10);

select
a.use_code
,b.county_building_use_description
,count(*) as records
,sum(residential_units) as du
,sum(gross_sqft) as gross_sqft
,sum(non_residential_sqft) as nonres_sqft
from buildings_20200512 a
inner join temp_king_split_use_codes b
on a.use_code = b.use_code
group by
a.use_code
,b.county_building_use_description;

select * from king_real_property_account where PredominantUse in ('459','597','830','840','847','848');

select * from king_apartment_complex where PredominantUse in ('459','597','830','840','847','848');
*/

-- Original processing script used 2014 building_use_generic_reclass - switching over to the updated 2018 version I made in February 2020

update buildings_20200519 a
Inner Join 2018_parcel_baseyear_working.building_use_generic_reclass_2018_v2 b
On a.use_code = b.county_building_use_code
set a.building_type_id = b.building_type_id
where b.county_id = 33;

-- MHS Additional cleanup after review 5-19-2020
-- drop nonres SQFT from obvious Residential only building types

update buildings_20200519
set non_residential_sqft = 0
where use_code in (352,300,338,348,351);  -- all MF or SF use codes 

-- drop Res Units where obvious units represents something else (hotel rooms, non-res condo spaces, storage units, theater screens?)

update buildings_20200519
set residential_units = 0
where use_code in (841,842,332,594,853,600,386,326,468,432,345,388,850,851,311,380,328,849);

-- doing what imputation I can on Mixed Use w/o Res Units
drop table if exists temp_mixeduse_sqft_per_unit_aggregate;
create temporary table temp_mixeduse_sqft_per_unit_aggregate
select
round(sum(gross_sqft-non_residential_sqft)/sum(residential_units),0) as sqft_per_unit
from buildings_20200519 where building_type_id = 10 and residential_units >0;

update buildings_20200519 a
inner join temp_mixeduse_sqft_per_unit_aggregate b
set a.residential_units = round((a.gross_sqft - a.non_residential_sqft)/b.sqft_per_unit,0)
where building_type_id = 10 and residential_units =0;

-- correcting for a very large condo complex uniquely represented in Assessors data with each record having no Gross SQFT, accurate SQFT per Unit estimates, and total complex units (770) under residential_units.  

SELECT @sum_imp_value:=sum(improvement_value)
,@max_du:=max(residential_units)
FROM buildings_20200519
where parcel_id in (
'7804190000'
,'7804220000'
,'7804230000'
,'7804240000'
,'7804180000'
,'7804210000'
,'7804170000'
);

Update buildings_20200519
set residential_units = round(improvement_value/@sum_imp_value*@max_du,0)
where parcel_id in (
'7804190000'
,'7804220000'
,'7804230000'
,'7804240000'
,'7804180000'
,'7804210000'
,'7804170000'
);

Update buildings_20200519
set gross_sqft = round(sqft_per_unit*residential_units,0)
where parcel_id in (
'7804190000'
,'7804220000'
,'7804230000'
,'7804240000'
,'7804180000'
,'7804210000'
,'7804170000'
);

-- changing building_id to auto increment as final step

alter table buildings_20200519 change column building_id building_id int(11) auto_increment primary key;


