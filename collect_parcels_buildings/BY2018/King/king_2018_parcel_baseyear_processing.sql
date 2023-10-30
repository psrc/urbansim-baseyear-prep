/*
Date: 1.6.2020
Author: Peter Caballero
Purpose: Assemble 2018 King County Parcels and Buildings tables

2018 Raw Assessor Files: 

Import raw asssessor files from access database to MySQL database: 
* Chris Peak imported raw assessor files

Import GIS processed files from
* J:\Projects\2018_base_year\King\GIS\PostProcessGIS\King.mdb

Two Union Square Building - 197670-0125
Waterfront Place Building - 919590-0000
Greenlake Storage - 153230-0095
	 select * from extr_aptcomplex where major = 153230 and minor = 0095\G
*/

/*	 
-- Stacked Parcel issue/QC

-- First stacked parcel issue - Find records where pin <> pin_1
drop table if exists tmp_pin_not_pin1;
create temporary table tmp_pin_not_pin1
select
	pin, 
	pin_1,
	count(*) as cnt
from prclpt_identity1
where
	pin <> pin_1
	and pin_1 <> ""
	and pin_1 not like "%trct%"
	and pin_1 not like "%hydr%"
	and pin_1 not like "%tr%"
	and pin not like "%trct%"
	and pin not like "%hydr%"
	and pin not like "%tr%"
group by 
	pin,
	pin_1;

alter table tmp_pin_not_pin1 add index pin_index(pin);
alter table tmp_pin_not_pin1 add index pin_1_index(pin_1);

select count(*) from prep_buildings_1 a 
	inner join tmp_pin_not_pin1 b on a.pin = b.pin 
where
	a.stacked_parcel_id = 0;

update prep_buildings_1 a 
	inner join tmp_pin_not_pin1 b on a.pin = b.pin 
set 
	a.stacked_flag = 1,
	a.stacked_parcel_id = b.pin_1,
	a.original_parcel_id = a.pin
where
	a.stacked_parcel_id = 0;

update prep_buildings_1 
set 
	pin = stacked_parcel_id
where
	stacked_flag = 1;
*/
	
/*	
-- Second stacked parcel issue - Find records where 
drop table if exists tmp_stacked_overlay;
create temporary table tmp_stacked_overlay 
	select a.pin, 
	min(b.pin) as min_pin, 
	count(*) as cnt 
from prclpt_identity1 a 
	inner join prep_parcels_1 b on a.pin = b.pin 
group by 
	a.pin
having cnt > 1;	 

alter table tmp_stacked_overlay add index pin_index(pin);
alter table tmp_stacked_overlay add index min_pin_index(min_pin);

drop table if exists tmp_double_entry;
create temporary table tmp_double_entry
select
	a.min_pin,
	b.pin,
	count(*) as cnt
from tmp_stacked_overlay a 
	inner join tmp_pin_not_pin1 b on a.min_pin = b.pin
group by
	a.min_pin,
	b.pin;
*/


	
	


--------------------------
-- Create indicies on relevant tables
--------------------------
alter table prcl add index pin_index(pin(10));
alter table prclpt add index pin_index(pin(10));
alter table extr_aptcomplex add index major_index(major(6));
alter table extr_aptcomplex add index minor_index(minor(4));
alter table extr_aptcomplex add index major_minor_index(major(6), minor(4));
alter table extr_commbldg add index major_index(major(6));
alter table extr_commbldg add index minor_index(minor(4));
alter table extr_commbldg add index major_minor_index(major(6), minor(4));
alter table extr_commbldg add index predominantuse_index(predominantuse);
alter table extr_commbldgfeature add index major_index(major(6));
alter table extr_commbldgfeature add index minor_index(minor(4));
alter table extr_commbldgfeature add index major_minor_index(major(6), minor(4));
alter table extr_commbldgsection add index major_index(major(6));
alter table extr_commbldgsection add index minor_index(minor(4));
alter table extr_commbldgsection add index major_minor_index(major(6), minor(4));
alter table extr_condocomplex add index major_index(major(6));
alter table extr_condounit2 add index major_index(major(6));
alter table extr_condounit2 add index minor_index(minor(4));
alter table extr_condounit2 add index major_minor_index(major(6), minor(4));
alter table extr_parcel add index major_index(major(6));
alter table extr_parcel add index minor_index(minor(4));
alter table extr_parcel add index major_minor_index(major(6), minor(4));
alter table extr_parcel add index present_use_index(presentuse);
alter table extr_parcel add column pin varchar(10);
	update extr_parcel set pin = concat(major, minor);
alter table extr_parcel add index pin_index(pin(10));
-- alter table extr_real_property_account add index major_index(major(6));
-- alter table extr_real_property_account add index minor_index(minor(4));
-- alter table extr_real_property_account add index major_minor_index(major(6), minor(4));
-- alter table extr_real_property_account add index pin_index(accountnumber(10));
alter table extr_resbldg add index major_index(major(6));
alter table extr_resbldg add index minor_index(minor(4));
alter table extr_resbldg add index major_minor_index(major(6), minor(4));
alter table extr_unitbreakdown add index major_index(major);
alter table extr_unitbreakdown add index minor_index(minor);
alter table extr_lookup add index lutype_index(lutype(10));

/*
alter table prcl add index pin_index(pin(10));
alter table prclpt add index pin_index(pin(10));
alter table king_apartment_complex add index major_index(major(6));
alter table king_apartment_complex add index minor_index(minor(4));
alter table king_apartment_complex add index major_minor_index(major(6), minor(4));
alter table king_commercial_building add index major_index(major(6));
alter table king_commercial_building add index minor_index(minor(4));
alter table king_commercial_building add index major_minor_index(major(6), minor(4));
alter table king_commercial_building add index predominantuse_index(predominantuse);
-- alter table extr_commbldgfeature add index major_index(major);
-- alter table extr_commbldgfeature add index minor_index(minor);
-- alter table extr_commbldgfeature add index major_minor_index(major, minor);
-- alter table extr_commbldgsection add index major_index(major);
-- alter table extr_commbldgsection add index minor_index(minor);
-- alter table extr_commbldgsection add index major_minor_index(major, minor);
alter table king_condo_complex add index major_index(major(6));
alter table king_condo_unit add index major_index(major(6));
alter table king_condo_unit add index minor_index(minor(4));
alter table king_condo_unit add index major_minor_index(major(6), minor(4));
alter table king_parcel add index major_index(major(6));
alter table king_parcel add index minor_index(minor(4));
alter table king_parcel add index major_minor_index(major(6), minor(4));
-- alter table king_parcel add index pin_index(pin(10));
alter table king_parcel add index present_use_index(presentuse);
alter table king_real_property_account add index major_index(major(6));
alter table king_real_property_account add index minor_index(minor(4));
alter table king_real_property_account add index major_minor_index(major(6), minor(4));
alter table king_real_property_account add index pin_index(accountnumber(10));
alter table king_residential_building add index major_index(major(6));
alter table king_residential_building add index minor_index(minor(4));
alter table king_residential_building add index major_minor_index(major(6), minor(4));
*/

--------------------------
-- Prep tables and join kinfnl15 (polygon) and kinptfnl15 (point) together
-- Purpose is to ensure that the x,y values from the point file have been appended/joined to the polygon file for ease of use
-- QC/QA check - both tables should have same amount of record
--------------------------
-- create land_use_generic_reclass table
drop table if exists land_use_generic_reclass;
create table land_use_generic_reclass
select 
	lutype as land_use_type,
	luitem as use_code,
	ludescription as use_desc
from extr_lookup;

alter table land_use_generic_reclass add index use_code_index(use_code);

-- Created summarized/grouped land/improvement value from extr_rpacct_noname
drop table if exists extr_rpacct_noname_grouped;
create table extr_rpacct_noname_grouped
select
	major,
	minor,
	taxstat,
	count(*) as count,
	sum(apprlandval) as apprlandval,
	sum(apprimpsval) as apprimpsval
from extr_rpacct_noname
group by
	major,
	minor;
	
alter table extr_rpacct_noname_grouped add index major_index(major);
alter table extr_rpacct_noname_grouped add index minor_index(minor);
alter table extr_rpacct_noname_grouped add index major_minor_index(major,minor);
alter table extr_rpacct_noname_grouped add column pin varchar(10);

update extr_rpacct_noname_grouped 
set pin = concat(major, minor);

alter table extr_rpacct_noname_grouped add index pin_index(pin(10));

-- Create prep_parcels_1 table
drop table if exists prep_parcels_1;
create table prep_parcels_1
select
	a.pin,
	b.point_x,
	b.point_y,
	a.poly_area,
	c.major,
	c.minor,
	c.propname,
	c.proptype,
	c.currentzoning,
	c.presentuse,
	c.sqftlot,
	c.nbrbldgsites,
	d.taxstat,
	d.apprlandval,
	d.apprimpsval
from prcl a
	left join prclpt b on a.pin = b.pin
	left join extr_parcel c on a.pin = c.pin
	left join extr_rpacct_noname_grouped d on a.pin = d.pin;

-- Check for duplicates - there should be same number of records in the prep_parcels_1 table as there are in the prcl table
/* -- Found 4 records where duplicates exists	
	create temporary table tmp1 select pin, count(*) as cnt from prep_parcels_1 group by pin having cnt > 1;
	pin (192007unkn, 885720trct) are the duplicates. 
	
*/
	
alter table prep_parcels_1 add index pin_index(pin);
alter table prep_parcels_1 add index major_index(major);
alter table prep_parcels_1 add index minor_index(minor);
alter table prep_parcels_1 add index major_minor_index(major,minor);
alter table prep_parcels_1 add index presentuse_index(presentuse);

-- Prepare prep_parcels_1 and prep_buildings_1 tables
alter table prep_buildings_1 add column stacked_parcel_id varchar(10);
alter table prep_buildings_1 add column original_parcel_id varchar(10);
alter table prep_buildings_1 add column stacked_flag int(11);

-- Begin process of updating prep_buildings_1 table's stacked xy records and same sized parcels
drop table if exists tmp_prep_stacked_sqft_xy;
create temporary table tmp_prep_stacked_sqft_xy
select
	poly_area as parcel_sqft_in_gis, 
	count(*) as cnt,
	point_x as x_coord_sp,
	point_y as y_coord_sp
from prep_parcels_1
group by
	poly_area,
	point_x,
	point_y
having cnt > 1;

alter table tmp_prep_stacked_sqft_xy add index parcel_sqft_index(parcel_sqft_in_gis);
alter table tmp_prep_stacked_sqft_xy add index x_coord_sp_index(x_coord_sp);
alter table tmp_prep_stacked_sqft_xy add index y_coord_sp_index(y_coord_sp);

-- Get original stacked parcel id from same sized, xy parcels
drop table if exists tmp_prep_stacked_sqft_xy_orig_parcel;
create temporary table tmp_prep_stacked_sqft_xy_orig_parcel
select
	a.*,
	b.pin as parcel_id_fips
from tmp_prep_stacked_sqft_xy a
	inner join prep_parcels_1 b on a.parcel_sqft_in_gis = b.poly_area and a.x_coord_sp = b.point_x and a.y_coord_sp = b.point_y;

alter table tmp_prep_stacked_sqft_xy_orig_parcel add index parcel_sqft_index(parcel_sqft_in_gis);
alter table tmp_prep_stacked_sqft_xy_orig_parcel add index x_coord_sp_index(x_coord_sp);
alter table tmp_prep_stacked_sqft_xy_orig_parcel add index y_coord_sp_index(y_coord_sp);
alter table tmp_prep_stacked_sqft_xy_orig_parcel add index parcel_id_fips_index(parcel_id_fips(10));	

-- Get minimum parcel_id value and set as new "base" parcel_id values for records where sizd and xy are the same
drop table if exists tmp_prep_stacked_sqft_xy_with_pin;
create temporary table tmp_prep_stacked_sqft_xy_with_pin
select
	min(b.pin) as parcel_id,
	a.parcel_sqft_in_gis,
	a.x_coord_sp,
	a.y_coord_sp,
	a.cnt
from tmp_prep_stacked_sqft_xy a 
	inner join prep_parcels_1 b on a.parcel_sqft_in_gis = b.poly_area and a.x_coord_sp = b.point_x and a.y_coord_sp = b.point_y
group by
	a.parcel_sqft_in_gis,
	a.x_coord_sp,
	a.y_coord_sp,
	a.cnt;

alter table tmp_prep_stacked_sqft_xy_with_pin add index parcel_id_index(parcel_id(10));	
alter table tmp_prep_stacked_sqft_xy_with_pin add index parcel_sqft_index(parcel_sqft_in_gis);
alter table tmp_prep_stacked_sqft_xy_with_pin add index x_coord_sp_index(x_coord_sp);
alter table tmp_prep_stacked_sqft_xy_with_pin add index y_coord_sp_index(y_coord_sp);
alter table tmp_prep_stacked_sqft_xy_with_pin add column original_parcel_id varchar(10);

-- Update stacked xy same sized parcels in the parcels table	
update prep_parcels_1 a 
	inner join tmp_prep_stacked_sqft_xy_with_pin b on a.poly_area = b.parcel_sqft_in_gis and a.point_x = b.x_coord_sp and a.point_y = b.y_coord_sp
set 
	a.stacked_parcel_id = b.parcel_id,
	a.original_parcel_id = a.pin,
	a.stacked_flag = 1;



-----------------------------
-- Create parcels table
-----------------------------
drop table if exists parcels_bak;
create table parcels_bak select * from parcels;

drop table if exists parcels;
create table parcels
	(parcel_id text,
	parcel_id_fips text,
	city_id int(11),
	zip_id int(11),
	plan_type_description text,
	plan_type_id_old int(11),
	grid_id int(11),
	id_plat int(11),
	y_coord_sp double,
	county_id int(11),
	plan_type_id int(11),
	school_district_id int(11),
	id_parcel text,
	is_inside_urban_growth_boundary int(11),
	x_coord_utm float,
	x_coord_sp double,
	zipcode text,
	duplicates int(11),
	num_building_records int(11),
	parcel_sqft int(11),
	genericlanduse1 text,
	regional_geography_id int(11),
	large_area_id int(11),
	is_duplicate int(11),
	land_value double,
	tax_exempt_flag int(11),
	parcel_sqft_in_gis int(11),
	faz_id int(11),
	faz_group_id int(11),
	zone_id int(11),
	land_use_type_id int(11),
	y_coord_utm float,
	census_2010_block_group_id text,
	census_2010_block_id text,
	use_code text,
	landuse_description text,
	exemption text,
	improvement_value double,
	stacked_flag int(11),
	stacked_parcel_id text,
	original_parcel_id text,
	address varchar(100)
	);


insert into parcels 
	(parcel_id_fips,
	land_value,
	improvement_value,
	use_code,
	exemption,
	parcel_sqft_in_gis,
	y_coord_sp,
	x_coord_sp,
	stacked_flag,
	stacked_parcel_id,
	original_parcel_id)
select
	pin as parcel_id_fips,
	apprlandval as land_value,
	apprimpsval as improvement_value,
	presentuse as use_code,
	taxstat as exemption,
	poly_area as parcel_sqft_in_gis,
	point_y as y_coord_sp,
	point_x as x_coord_sp,
	stacked_flag,
	stacked_parcel_id,
	original_parcel_id
from prep_parcels_1;

alter table parcels add index parcel_id_index(parcel_id(10));
alter table parcels add index use_code_index(use_code(10));

-- Tax Exemption flag
update parcels set tax_exempt_flag = 1 where exemption = 'X';

--------------------------
-- Parcel clean up
--------------------------
update parcels 
set city_id = 0 
where city_id is null;

update parcels
set zip_id = 0 
where zip_id is null;

update parcels
set plan_type_description = 0 
where plan_type_description is null;

update parcels 
set plan_type_id_old = 0
where plan_type_id_old is null;

update parcels
set grid_id = 0
where grid_id is null;

update parcels
set id_plat = 0
where id_plat is null;

update parcels
set y_coord_sp = 0 
where y_coord_sp is null;

update parcels
set county_id = 0
where county_id is null;

update parcels
set plan_type_id = 0
where plan_type_id is null;

update parcels
set school_district_id = 0
where school_district_id is null;

update parcels
set id_parcel = 0 
where id_parcel is null;

update parcels
set is_inside_urban_growth_boundary = 0
where is_inside_urban_growth_boundary is null;

update parcels
set x_coord_utm = 0 
where x_coord_utm is null;

update parcels 
set x_coord_sp = 0
where x_coord_sp is null;

update parcels
set zipcode = 0
where zipcode is null;

update parcels 
set duplicates = 0
where duplicates is null;

update parcels
set num_building_records = 0
where num_building_records is null;

update parcels
set parcel_sqft = 0
where parcel_sqft is null;

update parcels
set genericlanduse1 = 0 
where genericlanduse1 is null;

update parcels
set regional_geography_id = 0
where regional_geography_id is null;

update parcels
set large_area_id = 0
where large_area_id is null;

update parcels 
set is_duplicate = 0
where is_duplicate is null;

update parcels
set land_value = 0 
where land_value is null;

update parcels
set tax_exempt_flag = 0
where tax_exempt_flag is null;

update parcels
set parcel_sqft_in_gis = 0 
where parcel_sqft_in_gis is null;

update parcels
set faz_id = 0 
where faz_id is null;

update parcels
set faz_group_id = 0 
where faz_group_id is null;

update parcels
set zone_id = 0 
where zone_id is null;

update parcels
set land_use_type_id = 0
where land_use_type_id is null;

update parcels
set y_coord_utm = 0 
where y_coord_utm is null;

update parcels
set census_2010_block_group_id = 0 
where census_2010_block_group_id is null;

update parcels
set census_2010_block_id = 0 
where census_2010_block_id is null;

update parcels
set use_code = 0 
where use_code is null;

update parcels
set landuse_description = 0 
where landuse_description is null;

update parcels
set exemption = 0 
where exemption is null;

update parcels
set improvement_value = 0 
where improvement_value is null;

update parcels
set stacked_flag = 0 
where stacked_flag is null;

update parcels
set stacked_parcel_id = 0 
where stacked_parcel_id is null;

update parcels
set original_parcel_id = 0 
where original_parcel_id is null;

update parcels
set growth_center_id = 0
where growth_center_id is null;

-- update parcels
-- set census_2010_tract_id = 0
-- where census_2010_tract_id is null;

update parcels
set address = 0
where address is null;

update parcels a 
	inner join psrc_2014_parcel_baseyear_working.land_use_generic_reclass b on a.use_code = b.county_land_use_code
set 
	a.land_use_type_id = b.land_use_type_id
where
	b.county = 33;

--------------------------
-- Building assembly process
--------------------------

-- IMPORTANT 11.20.2015 - Have not run this since look up tables seems wonky, use 2011 building reclass table if need be

-- Create property_class/buiding use table
-- NOTE exported reclass table to access to insert generic buildings use 1 and 2 values (should only run this query 1 time)
/*
drop table if exists building_use_generic_reclass;	
create table building_use_generic_reclass
select
	,
	structure_use_code as use_code,
	structure_use_desc as use_desc,
	-- building_type as building_type,
	count(*) as count
from extr_commbldg
group by
	improvement_type,
	structure_use_code,
	structure_use_desc;
	-- building_type;
	
alter table building_use_generic_reclass add index use_code_index(use_code);
alter table building_use_generic_reclass add column generic_building_use_1 text;

update building_use_generic_reclass a 
	inner join psrc_2011_parcel_baseyear.building_use_generic_reclass b on a.use_code = b.county_building_use_code
set a.generic_building_use_1 = b.generic_building_use_1
where 
	b.county = 33;
	
-- Make some revisions to generic building use 1 (particularly NULL generic_building_use_1 values)

-- Update basEd on use_codes
update building_use_generic_reclass
set generic_building_use_1 = 'Commercial'
where use_code in ('Casino', 'COMBLDG', 'COMSHOP', 'FRATRNL', 'MORTUARY', 'MOTEL1SR', 'SERVICE', 'VETHOSP');

update building_use_generic_reclass
set generic_building_use_1 = 'Government'
where use_code = 'GOV CSB';

update building_use_generic_reclass
set generic_building_use_1 = 'Hospital / Convalescent Center'
where use_code in ('HOSPITAL', 'CONVHOSP');

update building_use_generic_reclass
set generic_building_use_1 = 'School'
where use_code in ('COLLEGE', 'LIBRARYC');

update building_use_generic_reclass
set generic_building_use_1 = 'Civic and Quasi-Public'
where use_code = 'MUSEUM';

update building_use_generic_reclass
set generic_building_use_1 = 'Parking'
where use_code = 'PARKING';

update building_use_generic_reclass
set generic_building_use_1 = 'Recreation'
where use_code = 'REC ENC';

-- Update based on use_descriptions
update building_use_generic_reclass
set generic_building_use_1 = 'Commercial'
where use_desc = 'Motel Room, 1 Sty-Single Row';

update building_use_generic_reclass
set generic_building_use_1 = 'Hospital / Convalescent Center'
where use_desc = 'Multiple Res-Retirement Com';

update building_use_generic_reclass
set generic_building_use_1 = 'School'
where use_desc = 'Vocational School';

update building_use_generic_reclass
set generic_building_use_1 = 'Single Family Residential'
where use_desc like '%Single family%';

update building_use_generic_reclass
set generic_building_use_1 = 'Multi-Family Residential'
where use_desc = 'Duplex';

update building_use_generic_reclass
set generic_building_use_1 = 'Multi-Family Residential'
where use_desc = 'Triplex';

update building_use_generic_reclass
set generic_building_use_1 = 'Multi-Family Residential'
where use_desc = '4-6 family';

update building_use_generic_reclass
set generic_building_use_1 = 'Multi-Family Residential'
where use_desc = 'Multi-family';

update building_use_generic_reclass
set generic_building_use_1 = 'Multi-Family Residential'
where use_desc = '5';

update building_use_generic_reclass
set generic_building_use_1 = 'Multi-Family Residential'
where use_desc like '%condo%';

update building_use_generic_reclass
set generic_building_use_1 = 'Multi-Family Residential'
where use_desc like '%townhouse%';

update building_use_generic_reclass
set generic_building_use_1 = 'Other'
where use_desc = 'Other residential';
*/

-- Create building_id field and auto_increment the field
-- alter table snohco_improvement_records_2014av add building_id int primary key auto_increment;


-- Create prep table for apartment complex records
drop table if exists prep_buildings_apt_1;
create table prep_buildings_apt_1
select
	major,
	minor,
	nbrstories as stories,
	nbrunits as units,
	avgunitsize as sqft_per_unit,
	bldgquality as building_quality,
	yrbuilt as year_built
from extr_aptcomplex;

alter table prep_buildings_apt_1 add column pin text;

update prep_buildings_apt_1
set pin = concat(major,minor);

alter table prep_buildings_apt_1 add index pin_index(pin(10));
alter table prep_buildings_apt_1 add index major_index(major);
alter table prep_buildings_apt_1 add index minor_index(minor);

-- Create prep table for commercial buildings records
drop table if exists prep_buildings_com_1;
create table prep_buildings_com_1
select
	major,
	minor,
	bldgnbr as building_number,
	nbrstories as stories,
	predominantuse as use_code,
	bldgquality as building_quality,
	bldggrosssqft as bldg_gross_sqft,
	bldgnetsqft as bldg_net_sqft,
	yrbuilt as year_built
from extr_commbldg;

alter table prep_buildings_com_1 add column pin text;
alter table prep_buildings_com_1 add column residential_units int(11);
alter table prep_buildings_com_1 add column sqft_per_unit int(11);

update prep_buildings_com_1
set pin = concat(major,minor);

alter table prep_buildings_com_1 add index pin_index(pin(10));
alter table prep_buildings_com_1 add index major_index(major);
alter table prep_buildings_com_1 add index minor_index(minor);
alter table prep_buildings_com_1 add index building_number_index(building_number);

-- Create prep table for condo colmplex building records
drop table if exists prep_buildings_condo_1;
create table prep_buildings_condo_1
select
	major,
	nbrstories as stories,
	nbrunits as units,
	avgunitsize as sqft_per_unit,
	bldgquality as building_quality,
	yrbuilt as year_built
from extr_condocomplex;

alter table prep_buildings_condo_1 add column pin text;
alter table prep_buildings_condo_1 add column land_value int(11);
alter table prep_buildings_condo_1 add column improvement_value int(11);
alter table prep_buildings_condo_1 add column minor varchar(255) after major;

update prep_buildings_condo_1
set minor = '0000';

update prep_buildings_condo_1
set pin = concat(major,minor);

alter table prep_buildings_condo_1 add index pin_index(pin(10));
alter table prep_buildings_condo_1 add index major_index(major);
alter table prep_buildings_condo_1 add index minor_index(minor);

-- Get records where improvement value is null
drop table if exists tmp_find_condo_value;
create temporary table tmp_find_condo_value
select 
	a.*,
	b.apprlandval,
	b.apprimpsval,
	b.taxstat
from extr_condounit2 a
	inner join extr_rpacct_noname_grouped b on a.major = b.major and a.minor = b.minor;

alter table tmp_find_condo_value add column pin text;

update tmp_find_condo_value 
set pin = concat(major,minor);

alter table tmp_find_condo_value add index major_index(major);
alter table tmp_find_condo_value add index minor_index(minor);
alter table tmp_find_condo_value add index pin_index(pin(10));

drop table if exists tmp_condo_bldgs_calc_imp_value;
create temporary table tmp_condo_bldgs_calc_imp_value
select
	major,
	sum(apprlandval) as land_value,
	sum(apprimpsval) as improvement_value
from tmp_find_condo_value
group by	
	major;
	
alter table tmp_condo_bldgs_calc_imp_value add index major_index(major);

update prep_buildings_condo_1 a 
	inner join tmp_condo_bldgs_calc_imp_value b on a.major = b.major
set
	a.land_value = b.land_value,
	a.improvement_value = b.improvement_value;


--------------------------
-- Adjust Apartment and Commercial buildings where PINs match
-- NOTE - some condo complex records can be found in apartment records e.g. major = 919590 minor = 0000 1011 western ave!
--------------------------

-- Determine which records match/don't match to prep_buildings_com_1 table
drop table if exists tmp_aptcom;
create temporary table tmp_aptcom
select
	a.pin,
	count(*) as count
from prep_buildings_apt_1 a
	inner join prep_buildings_com_1 b on a.pin = b.pin
group by
	a.pin;
	
drop table if exists tmp_aptcom_not;
create temporary table tmp_aptcom_not
select 
	a.*
from prep_buildings_apt_1 a
	left join prep_buildings_com_1 b on a.pin = b.pin 
where 
	b.pin is null;
	
-- Start process for determing how many residential units need to be allocated to different building records in the extr_commbldgs table
drop table if exists tmp_aptcom_combo;
create temporary table tmp_aptcom_combo
select
	a.units,
	a.sqft_per_unit as sqft_per_unit_apt,
	b.*
from prep_buildings_apt_1 a 
	inner join prep_buildings_com_1 b on a.pin = b.pin;

alter table tmp_aptcom_combo add column proportion_1 double;

-- create table of bldg sqft of proportion for allocation of units
drop table if exists tmp_aptcom_combo_prop;
create temporary table tmp_aptcom_combo_prop
select
	pin,
	sum(bldg_gross_sqft) as gross_sqft,
	count(*) as count
from tmp_aptcom_combo 
group by
	pin;
	
alter table tmp_aptcom_combo_prop add index pin_index(pin(10));

update tmp_aptcom_combo a 
	inner join tmp_aptcom_combo_prop b on a.pin = b.pin
set a.proportion_1 = a.bldg_gross_sqft/b.gross_sqft;

update tmp_aptcom_combo
set residential_units = (units*proportion_1)
where 
	proportion_1 is not null;

-- update prep_buildings_com_1 table to reflect individual units per building
update prep_buildings_com_1 a 
	inner join tmp_aptcom_combo b on a.pin = b.pin and a.building_number = b.building_number
set a.residential_units = b.residential_units;

/*
-- update prep_buildings_com_1 table to reflect "average" sqft_per_unit value
-- Determined that too difficult to select each buildings sqft per unit value since some records have correct net_sqft value for calculation, but other records do not
update prep_buildings_com_1 a 
	inner join tmp_aptcom_combo b on a.pin = b.pin and a.building_number = b.building_number
set a.sqft_per_unit = b.sqft_per_unit_apt;
*/
--------------------------
-- Adjust Condo Complex and Commercial buildings where PINs match

-- IMPORTANT do not need to run bc apartment records accounted for residential units and sqft per unit values - why they are in both tables is odd
-- But if ever need to run this the query below should work
-- NOTE - some condo complex records can be found in apartment records e.g. major = 919590 minor = 0000 1011 western ave!
--------------------------

drop table if exists tmp_condocom;
create temporary table tmp_condocom
select
	a.major,
	count(*) as count
from prep_buildings_condo_1 a
	inner join prep_buildings_com_1 b on a.major = b.major
group by
	a.major;
	
drop table if exists tmp_condocom_not;
create temporary table tmp_condocom_not
select 
	a.*
from prep_buildings_condo_1 a
	left join prep_buildings_com_1 b on a.major = b.major
where 
	b.major is null;
	
-- Start process for determing how many residential units need to be allocated to different building records in the extr_commbldgs table
drop table if exists tmp_condocom_combo;
create temporary table tmp_condocom_combo
select
	a.units,
	a.sqft_per_unit as sqft_per_unit_apt,
	b.*
from prep_buildings_condo_1 a 
	inner join prep_buildings_com_1 b on a.major = b.major;

alter table tmp_condocom_combo add column proportion_1 double;

-- create table of bldg sqft of proportion for allocation of units
drop table if exists tmp_condocom_combo_prop;
create temporary table tmp_condocom_combo_prop
select
	major,
	sum(bldg_gross_sqft) as gross_sqft,
	count(*) as count
from tmp_condocom_combo 
group by
	major;
	
alter table tmp_condocom_combo_prop add index major_index(major(10));

update tmp_condocom_combo a 
	inner join tmp_condocom_combo_prop b on a.major = b.major
set a.proportion_1 = a.bldg_gross_sqft/b.gross_sqft;

update tmp_condocom_combo
set residential_units = (units*proportion_1)
where 
	proportion_1 is not null;

-- update prep_buildings_com_1 table to reflect individual units per building
update prep_buildings_com_1 a 
	inner join tmp_condocom_combo b on a.major = b.major and a.building_number = b.building_number
set a.residential_units = b.residential_units
where 
	a.residential_units is null;

/*	
-- update prep_buildings_com_1 table to reflect "average" sqft_per_unit value
-- Determined that too difficult to select each buildings sqft per unit value since some records have correct net_sqft value for calculation, but other records do not
update prep_buildings_com_1 a 
	inner join tmp_condocom_combo b on a.major = b.major and a.building_number = b.building_number
set a.sqft_per_unit = b.sqft_per_unit_apt;
*/

--------------------------
-- Create prep_buildings_res_1 table 
--------------------------
drop table if exists prep_buildings_res_1;
create table prep_buildings_res_1
select
	major,
	minor,
	bldgnbr,
	nbrlivingunits,
	address,
	stories,
	sqfttotliving,
	yrbuilt,
	`condition` as building_quality_id,
	concat(major,minor) as pin
from extr_resbldg;

alter table prep_buildings_res_1 add index pin_index(pin(10));
alter table prep_buildings_res_1 add column use_code int(11);

update prep_buildings_res_1
set use_code = 1
where nbrlivingunits = 1;

update prep_buildings_res_1
set use_code = 2
where nbrlivingunits = 2;

update prep_buildings_res_1
set use_code = 3
where nbrlivingunits = 3;

update prep_buildings_res_1
set use_code = 4
where nbrlivingunits = 4;

update prep_buildings_res_1
set use_code = 5
where nbrlivingunits = 5;

update prep_buildings_res_1
set use_code = 6
where nbrlivingunits = 6;

update prep_buildings_res_1
set use_code = 9
where nbrlivingunits = 9;

update prep_buildings_res_1
set use_code = 20
where nbrlivingunits = 20;

alter table prep_buildings_res_1 add index use_code_index(use_code);

--------------------------
-- Create prep_buildings_1 table 
-------------------------- 
drop table if exists prep_buildings_1;
create table prep_buildings_1
	(major text,
	minor text,
	pin text,
	building_id int(11),
	improvement_value int(11),
	stories int(11),
	residential_units int(11),
	year_built int(11),
	sqft_per_unit int(11),
	gross_sqft int(11),
	non_residential_sqft int(11),
	building_quality_id int(11),
	use_code int(11));

alter table prep_buildings_1 add index pin_index(pin(10));
-- Insert residential building records
insert into prep_buildings_1
	(major,
	minor,
	pin,
	stories,
	residential_units,
	year_built,
	gross_sqft,
	sqft_per_unit,
	building_quality_id,
	use_code)
select
	major,
	minor,
	pin,
	stories,
	nbrlivingunits as residential_units,
	yrbuilt as year_built,
	sqfttotliving as gross_sqft,
	sqfttotliving as sqft_per_unit,
	building_quality_id,
	use_code
from prep_buildings_res_1;

-- Insert commercial (with units) building records
insert into prep_buildings_1
	(major,
	minor,
	pin,
	stories,
	residential_units,
	year_built,
	gross_sqft,
	non_residential_sqft,
	sqft_per_unit,
	building_quality_id,
	use_code)
select
	major,
	minor,
	pin,
	stories,
	residential_units as residential_units,
	year_built as year_built,
	bldg_gross_sqft as gross_sqft,
	bldg_net_sqft as non_residential_sqft,
	sqft_per_unit as sqft_per_unit,
	building_quality as building_quality_id,
	use_code as use_code
from prep_buildings_com_1;

-- Insert condo complex building records
insert into prep_buildings_1
	(major,
	minor,
	pin,
	stories,
	residential_units,
	year_built,
	sqft_per_unit,
	building_quality_id,
	use_code)
select
	a.major,
	a.minor,
	a.pin,
	a.stories,
	a.units as residential_units,
	a.year_built as year_built,
	a.sqft_per_unit as sqft_per_unit,
	a.building_quality as building_quality_id,
	800 as use_code
from prep_buildings_condo_1 a left join prep_buildings_com_1 b on a.pin = b.pin where b.pin is null;

-- Insert apartment complex building records
insert into prep_buildings_1
	(major,
	minor,
	pin,
	stories,
	residential_units,
	year_built,
	sqft_per_unit,
	building_quality_id,
	use_code)
select
	a.major,
	a.minor,
	a.pin,
	a.stories,
	a.units as residential_units,
	a.year_built as year_built,
	a.sqft_per_unit as sqft_per_unit,
	a.building_quality as building_quality_id,
	300 as use_code
from prep_buildings_apt_1 a left join prep_buildings_1 b on a.pin = b.pin where b.pin is null;

-- alter table prep_buildings_1 add index pin_index(pin(10));
alter table prep_buildings_1 add index building_id_index(building_id);
alter table prep_buildings_1 add index use_code_index(use_code);

-- Calculate proportion improvement value for building records
alter table prep_buildings_1 add column proportion double;

drop table if exists tmp_bldgs_impvalue;

create temporary table tmp_bldgs_impvalue
select
	parcel_id_fips as parcel_id, 
	sum(ifnull(improvement_value,0)) as improvement_value
from parcels 
group by
	parcel_id_fips;

alter table tmp_bldgs_impvalue add index parcel_id_index(parcel_id(10));

-- Create sum total building sqft by pin
drop table if exists tmp_bldgs_sqft;

create temporary table tmp_bldgs_sqft
select
	pin,
	count(*) as count,
	sum(gross_sqft) as total_sqft
from prep_buildings_1
group by
	pin;

alter table tmp_bldgs_sqft add index pin_index(pin(10));

-- Calculate proportion
update prep_buildings_1 a
	inner join tmp_bldgs_sqft b on a.pin = b.pin
set a.proportion = a.gross_sqft/b.total_sqft;

update prep_buildings_1 a 
	inner join tmp_bldgs_impvalue b on a.pin = b.parcel_id
set a.improvement_value = a.proportion * b.improvement_value;

-- Update condo (and potentially other commercial building records' improvement values)
update prep_buildings_1 a 
	inner join prep_buildings_condo_1 b on a.major = b.major and a.minor = b.minor
set a.improvement_value = b.improvement_value 
where 
	a.improvement_value is null 
	or a.improvement_value = 0;

-- Update the parcels table land_value where improvement value is null based on condo valuation info
update prep_parcels_1 a 
	inner join prep_buildings_condo_1 b on a.pin = b.pin
set a.apprlandval = b.land_value
where
	a.apprlandval is null 
	or a.apprlandval = 0;

-- STOPPED WORK HERE 11.23.2015

--------------------------
-- Stacked, overlapping assembly process
/*
GIS Steps: 
	1. convert \\FILE2\DataTeam\Projects\UrbanSim\NEW_DIRECTORY\GIS\Shapefiles\Parcels\Snohomish\2015\master.gdb\draft_snofnlpt15_nonbase
	2. spatial overlay \\FILE2\DataTeam\Projects\UrbanSim\NEW_DIRECTORY\GIS\Shapefiles\Parcels\Snohomish\2015\snofnlpt15_nonbase.shp and \\FILE2\DataTeam\Projects\UrbanSim\NEW_DIRECTORY\GIS\Shapefiles\Parcels\Snohomish\2015\draft1\kinfnl15.shp
		output \\FILE2\DataTeam\Projects\UrbanSim\NEW_DIRECTORY\GIS\Shapefiles\Parcels\Snohomish\2015\snofnlpt15_nonbase_tobase.shp
	3. export to J:\Projects\UrbanSim\NEW_DIRECTORY\Databases\Access\Parcels\Snohomish\2015\downloads\May_12_2015\assr_roll.mdb
	4. export to snohomish_2014_parcel_baseyear
*/
--------------------------
/*
Fix stacked xy parcels - run J:\projects\UrbanSim\NEW_DIRECTORY\Databases\Access\Parcels\King\2015\downloads\April_9_2015\king_april_2015_stacked_xy_parcels_revamp.txt
Fix overlapping parcels - run J:\projects\UrbanSim\NEW_DIRECTORY\Databases\Access\Parcels\King\2015\downloads\April_9_2015\king_april_2015_overlapping_parcels_revamp.txt

1. Once the stacked_parcels_all_found_base and overlapping_filtered_least_with_stacked_records tables have been created
2. Update the prep_buildings_1 table via the stacked xy
	* Updates records where stacked to "base" pin (stacked records simply becomes another building records)
3. Update the prep_buildings_1 table via the overlapping
	* Updates records that were overlapping to "least" pin (overlapping records simply becomes another building records)
4. Update the parcels table
	* Executive decision to ADD land value of overlapping parcel to "least" pin record
5. Clean up parcels table and delete stacked or overlapping records

*/

		
-- Update stacked building records based on xy same sized parcels;
update prep_buildings_1 a 
	inner join prep_parcels_1 b on a.pin = b.pin
set 
	a.stacked_parcel_id = b.stacked_parcel_id,
	a.original_parcel_id = a.pin,
	a.stacked_flag = 1
where 
	b.stacked_flag = 1;
	
-- Find remaining overlaping and stacked parcels
-- QC - prclpt_identity1 has 5116 records where the pin <> pin_1
drop table if exists tmp_pin_not_pin1;
create temporary table tmp_pin_not_pin1
select
	pin, 
	pin_1,
	count(*) as cnt
from prclpt_identity1
where
	pin <> pin_1
	and pin_1 <> ""
	and pin_1 not like "%trct%"
	and pin_1 not like "%hydr%"
	and pin_1 not like "%tr%"
	and pin not like "%trct%"
	and pin not like "%hydr%"
	and pin not like "%tr%"
group by 
	pin,
	pin_1;

alter table tmp_pin_not_pin1 add index pin_index(pin);
alter table tmp_pin_not_pin1 add index pin_1_index(pin_1);

-- Update stacked pins
update prep_buildings_1 a 
	inner join tmp_pin_not_pin1 b on a.pin = b.pin 
set 
	a.stacked_flag = 1,
	a.stacked_parcel_id = b.pin_1,
	a.original_parcel_id = a.pin
where
	a.stacked_flag is null;

-- Replace original pin with stacked pins
update prep_buildings_1 
set 
	pin = stacked_parcel_id
where
	stacked_flag = 1;

	
-- Update prep_parcels_1 
/*
-- Get the base (or minimum parcel id) parcel from the overlaping records
drop table if exists tmp_prep_remaining_stacked_parcels_min_pin;
create temporary table tmp_prep_remaining_stacked_parcels_min_pin
select
	min(
-- Update overlapping parcel records to prep_buildings_1 table to retain original parcel id
-- On 12.15.2015 this did not update any records so not sure if it's necessary bc least_pin record might already be captured in the stacked_parcels_all_found_base.base_pin table
alter table prep_buildings_1 add column overlap_parcel_id varchar(10);

update prep_buildings_1 a 
	inner join overlapping_filtered_least_with_stacked_records b on a.stacked_parcel_id = b.pin
set a.overlap_parcel_id = b.pin;

update prep_buildings_1 a 
	inner join overlapping_filtered_least_with_stacked_records b on a.pin = b.pin
set a.pin = b.least_pin;
*/


--- STOPPED HERE 12.15.2015: i re-assigned pins in the prep_buildings_1 table 
-- 1. now need to create the buildings table 
-- 2. and add land value to base_parcels in parcels table 
-- 3. and delete records

-- Begin creating buildings table
drop table if exists buildings_bak;

create table buildings_bak
select * 
from buildings;

drop table if exists buildings;
create table buildings
	(building_id int(11),
	gross_sqft int(11),
	non_residential_sqft int(11),
	year_built int(11),
	parcel_id varchar(20),
	job_capacity int(11),
	land_area int(11),
	building_quality_id int(11),
	improvement_value int(11),
	stories int(11),
	tax_exempt int(11),
	building_type_id int(11),
	template_id int(11),
	sqft_per_unit int(11),
	residential_units int(11),
	not_demolish int(11),
	use_code varchar(255),
	stacked_flag int(11),
	stacked_parcel_id varchar(20),
	original_parcel_id varchar(20)
	);

insert into buildings
	(gross_sqft,
	non_residential_sqft,
	sqft_per_unit,
	year_built,
	parcel_id,
	residential_units,
	improvement_value,
	use_code,
	stories,
	stacked_flag,
	stacked_parcel_id,
	original_parcel_id)
select
	gross_sqft,
	non_residential_sqft,
	sqft_per_unit,
	year_built,
	pin as parcel_id,
	residential_units,
	improvement_value,
	use_code,
	stories,
	stacked_flag,
	stacked_parcel_id,
	original_parcel_id
from prep_buildings_1;

alter table buildings add index parcel_id_index(parcel_id(10));
alter table buildings add index building_id_index(building_id);
alter table buildings add index use_code_index(use_code(10));

-- alter table buildings change column building_id building_id int(11) auto_increment primary key;

-- --------------------------
-- Clean up buildings table fields
-- --------------------------
update buildings
set gross_sqft = 0
where gross_sqft is null;

update buildings
set non_residential_sqft = 0
where non_residential_sqft is null;

update buildings
set year_built = 0
where year_built is null;

update buildings
set job_capacity = 0 
where job_capacity is null;

update buildings
set land_area = 0 
where land_area is null;

update buildings 
set building_quality_id = 0 
where building_quality_id is null;

update buildings
set improvement_value = 0
where improvement_value is null;

update buildings 
set stories = 0 
where stories is null;

update buildings
set tax_exempt = 0 
where tax_exempt is null;

update buildings
set building_type_id = 0 
where building_type_id is null;

update buildings 
set template_id = 0
where template_id is null;

update buildings
set sqft_per_unit = 0
where sqft_per_unit is null;

update buildings
set residential_units = 0
where residential_units is null;

update buildings 
set not_demolish = 0
where not_demolish is null;

update buildings 
set use_code = 0
where use_code is null;

update buildings
set stacked_flag = 0
where stacked_flag is null;

update buildings
set stacked_parcel_id = 0
where stacked_parcel_id is null;

update buildings
set original_parcel_id = 0
where original_parcel_id is null;

update buildings a 
	inner join psrc_2014_parcel_baseyear_working.building_use_generic_reclass b on a.use_code = b.county_building_use_code
set 
	a.building_type_id = b.building_type_id
where
	b.county = 33;

-- --------------------------
-- Delete records from buildings table that do not exists in parcels table
-- --------------------------
-- delete 34 records
drop table if exists buildings_before_delete_not_in_parcels;

create table buildings_before_delete_not_in_parcels
select *
from buildings;

delete a.*
from buildings a left join parcels b on a.parcel_id = b.parcel_id
where b.parcel_id is null;
	
-- --------------------------
-- Delete records from parcels table that do not exists in the parcel shapefile (aka kinptfnl15)
-- --------------------------
drop table if exists parcels_before_delete_not_in_gis;

create table parcels_before_delete_not_in_gis
select *
from parcels;

-- 0 records were deleted, which makes sense since the kinptfnl15 file was the dissolved, stacked, polygon file so there should be little to no duplicates, stackes, etc.
delete a.*
from parcels a left join kinptfnl15 b on a.parcel_id = b.pin
where b.pin is null;

-- Create county Id column on buildings table
alter table buildings add column county_id int(11);

update buildings
set county_id = 33;

alter table buildings add index county_id_index(county_id);
-- DONE BUILDINGS PARCELS AND BUILDINGS TABLES --

/*
-- Fix parcels table stacked/overlapping records
drop table if exists tmp_stacked_overlapping_parcels;
create temporary table tmp_stacked_overlapping_parcels
select
	pin as parcel_id,
	stacked_parcel_id
from prep_buildings_1
where stacked_parcel_id is not null;

alter table tmp_stacked_overlapping_parcels add index parcel_id_index(parcel_id(10));
alter table tmp_stacked_overlapping_parcels add index stacked_parcel_id_index(stacked_parcel_id(10));

drop table if exists parcels_bak;
create table parcels_bak
select *
from parcels;

update parcels a
	inner join tmp_stacked_overlapping_parcels b on a.parcel_id = b.stacked_parcel_id
/*
-- Create backup of "stacked/overlapping" shapefile 
alter table snoflnpt15_nonbase_tobase add index parcel_id_index(parcel_id);

-- Must re-import file from Access if snoflnpt15_nonbase_tobase_bak already exists bc that means I've already delete files from the original
drop table if exists snoflnpt15_nonbase_tobase_bak;
create table snoflnpt15_nonbase_tobase_bak
select *
from snoflnpt15_nonbase_tobase;

-- Find records from "stacked/overlapping" shapefile that already exists in base parcel shapefile
drop table if exists prep_stacked_buildings_already_exists_tobe_deleted_1;
create table prep_stacked_buildings_already_exists_tobe_deleted_1
select 
	a.* 
from snoflnpt15_nonbase_tobase a 
	inner join kinfnl15 b on a.parcel_id = b.parcel_id;
	
alter table prep_stacked_buildings_already_exists_tobe_deleted_1 add index parcel_id_index(parcel_id);

delete a.*
from snoflnpt15_nonbase_tobase a 
	inner join kinfnl15 b on a.parcel_id = b.parcel_id;

-- Begin assembling the stacked parcels and associating it with the processed prep_buildings_1 table
drop table if exists prep_stacked_buildings_1;
create table prep_stacked_buildings_1
select
	a.parcel_id as stacked_pin,
	a.PARCEL_I_1 as base_parcel,
	a.mklnd as land_value_orig,
	a.mkimp as improvement_value_orig,
	a.mkttl as total_value_orig,
	a.x_coord,
	a.y_coord,
	b.*	
from snoflnpt15_nonbase_tobase a 
	left join prep_buildings_1 b on a.parcel_id = b.pin;

alter table prep_stacked_buildings_1 add index stacked_pin_index(stacked_pin);
alter table prep_stacked_buildings_1 add index building_id_index(building_id);

-- Update improvement value
update prep_stacked_buildings_1
set improvement_value = (improvement_value_orig * proportion)
where improvement_value is null;

-- Adjust Stacked Single Family units set number_of_rooms = 1 where number_of_rooms > 1
update prep_stacked_buildings_1 a 
	inner join building_use_generic_reclass b on a.structure_use_code = b.use_code
set a.number_of_rooms = 1
where 
	b.generic_building_use_1 = 'Single Family Residential';

-- Remove NULL value from prep_buildings_1
update prep_buildings_1
set structure_use_code = 0
where structure_use_code is null;

-- Check if base_parcel is found in base polygon shapefile - ALL records should be found!
-- 8 records cannot be found
select 
	count(*) 
from prep_stacked_buildings_1 a 
	inner join kinfnl15 b on a.base_parcel = b.parcel_id;

-- Begin creating buildings table
drop table if exists buildings_bak;

create table buildings_bak
select * 
from buildings;

drop table if exists buildings;
create table buildings
	(building_id int(11),
	non_residential_sqft int(11),
	year_built int(11),
	parcel_id text,
	job_capacity int(11),
	land_area int(11),
	building_quality_id int(11),
	improvement_value int(11),
	stories int(11),
	tax_exempt int(11),
	building_type_id int(11),
	template_id int(11),
	sqft_per_unit int(11),
	residential_units int(11),
	not_demolish int(11),
	use_code varchar(255),
	stacked_pin text);

-- Insert residential portion of prep_buildings_1 into buildings table
insert into buildings
	(building_id,
	sqft_per_unit,
	year_built,
	parcel_id,
	residential_units,
	improvement_value,
	use_code,
	stories)
select
	building_id as building_id,
	(sqft/number_of_rooms) as sqft_per_unit,
	year_built as year_built,
	pin as parcel_id,
	number_of_rooms as residential_units,
	improvement_value,
	structure_use_code as use_code,
	stories as stories
from prep_buildings_1
where
	structure_use_code in ('APART', '1', '11', '12', '13', '2', '3', '4', '44', '5', '51', '52', '53', '61', '62', '63');

-- Insert non-residential buildings into buildings table
insert into buildings
	(building_id,
	non_residential_sqft,
	year_built,
	parcel_id,
	improvement_value,
	use_code,
	stories)
select
	building_id,
	sqft as non_residential_sqft,
	year_built as year_built,
	pin as parcel_id,
	improvement_value,
	structure_use_code as use_code,
	stories as stories
from prep_buildings_1 a 
	
where
	structure_use_code not in ('APART', '1', '11', '12', '13', '2', '3', '4', '44', '5', '51', '52', '53', '61', '62', '63');

alter table buildings add index parcel_id_index(parcel_id(10));
alter table buildings add index building_id_index(building_id);
alter table buildings add index use_code_index(use_code(10));

-- Update "stacked/overlapping" records' improvement_value in buildings table 
update buildings a 
	inner join prep_stacked_buildings_1 b on a.parcel_id = b.stacked_pin and a.building_id = b.building_id
set a.improvement_value = b.improvement_value;

-- Update "stacked/overlapping" records stacked_pin field 
	-- it's not updating 5,888 records... why? 
update buildings a 
	inner join prep_stacked_buildings_1 b on a.parcel_id = b.stacked_pin and a.building_id = b.building_id
set a.stacked_pin = b.stacked_pin;

	-- Diagnose 5,888 records
	/*
	drop table if exists tmp_missing_stacked_records;
	create temporary table tmp_missing_stacked_records
	select
		a.*
	from prep_stacked_buildings_1 
		a left join buildings b on a.stacked_pin = b.parcel_id and a.building_id = b.building_id
	where 
		b.parcel_id is null and b.building_id is null;	
		
	alter table tmp_missing_stacked_records add index stacked_pin_index(stacked_pin);
	alter table tmp_missing_stacked_records add index building_id_index(building_id);
	
	select
		count(*)
	from tmp_missing_stacked_records a 
		left join snohco_improvement_records_2014av b on a.stacked_pin = b.pin and a.building_id = b.building_id
	where 
		b.pin is null and b.building_id is null;
	
	Discovered that PIN value in the table is no so there are no improvement records for these stacked pins 
	
	*/

update buildings a 
	inner join prep_stacked_buildings_1 b on a.parcel_id = b.stacked_pin and a.building_id = b.building_id
set a.parcel_id = b.base_parcel;

-- The base parcels for some records are missing land value, use "stacked/overlapping" records to calculate base land value if it doesn't exist
	-- remember the land_value_orig from prep_stacked_buildings_1 originated from the tax_account table

drop table if exists prep_stacked_buildings_land_value_1;
create table prep_stacked_buildings_land_value_1
select
	-- stacked_pin,
	base_parcel,
	sum(improvement_value_orig) as improvement_value,
	sum(land_value_orig) as land_value
from prep_stacked_buildings_1
group by
	-- stacked_pin,
	base_parcel;
	
-- alter table prep_stacked_buildings_land_value_1 add index stacked_pin_index(stacked_pin);
alter table prep_stacked_buildings_land_value_1 add index based_parcel_index(base_parcel);
	
update parcels a 
	inner join prep_stacked_buildings_land_value_1 b on a.parcel_id = b.base_parcel
set a.land_value = b.land_value
where a.land_value = 0;
--------------------------
-- Clean up buildings table fields
--------------------------
update buildings
set non_residential_sqft = 0
where non_residential_sqft is null;

update buildings
set year_built = 0
where year_built is null;

update buildings
set job_capacity = 0 
where job_capacity is null;

update buildings
set land_area = 0 
where land_area is null;

update buildings 
set building_quality_id = 0 
where building_quality_id is null;

update buildings
set improvement_value = 0
where improvement_value is null;

update buildings 
set stories = 0 
where stories is null;

update buildings
set tax_exempt = 0 
where tax_exempt is null;

update buildings
set building_type_id = 0 
where building_type_id is null;

update buildings 
set template_id = 0
where template_id is null;

update buildings
set sqft_per_unit = 0
where sqft_per_unit is null;

update buildings
set residential_units = 0
where residential_units is null;

update buildings 
set not_demolish = 0
where not_demolish is null;

update buildings 
set use_code = 0
where use_code is null;

update buildings
set stacked_pin = 0
where stacked_pin is null;

--------------------------
-- Delete records from buildings table that do not exists in parcels table
-- 2.9.2020 49 records removed
--------------------------
drop table if exists buildings_before_delete_not_in_parcels;

create table buildings_before_delete_not_in_parcels
select *
from buildings;

delete a.*
from buildings a left join parcels b on a.parcel_id = b.parcel_id_fips
where b.parcel_id_fips is null;
	
--------------------------
-- Delete records from parcels table that do not exists in the parcel shapefile (aka kinptfnl15)
-- 2.9.2020 0 records removed
--------------------------
drop table if exists parcels_before_delete_not_in_gis;

create table parcels_before_delete_not_in_gis
select *
from parcels;

delete a.*
from parcels a left join prcl b on a.parcel_id_fips = b.pin
where b.pin is null;


11.5.2015 I need to QC records that were deleted...
