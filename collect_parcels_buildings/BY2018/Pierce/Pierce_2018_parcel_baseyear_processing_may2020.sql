/*
Date: 2-5-2020
Author: Peter Caballero
Reviewer: Hana Sevcikova
Review Date: 04-27-2020
Purpose: Assemble 2018 Pierce County Parcels and Buildings tables

(HS) The path below needs to be updated

2015 Raw Assessor Files:

Import raw asssessor files from access database to MySQL database: J:\Projects\UrbanSim\NEW_DIRECTORY\Databases\Access\Parcels\Pierce\2015\downloads\April_9_2015\prelim_pierce_2015_April9_parcels.accdb
*/

/*
(HS) Here is the set of tables I think is needed for this script:

   base_parcel_dissolve
   base_parcel_dissolve_pt
   nonbase_parcel_dissolve
   nonbase_to_base (This table gets modified in this script. The original can be copied as follows:)
	                	create table nonbase_to_base select *
		                from pierce_2018_parcel_baseyear.nonbase_to_base_bak;
                    alter table nonbase_to_base add index taxparceln_index (taxparceln), add index taxparce_1_index (taxparce_1);
   tax_parcels_project
   pierce_tax_account (how does it relate to tax_parcels_project?)
   pierce_improvement
   pierce_improvement_builtas (what is the difference between this and pierce_improvement?)
   land_use_generic_reclass - code for creating it is below in this script (commented out)

Note: I do not really understand the whole stacked parcel business. Could I get it explained simply?
(PMC) A: stacked parcels are parcel records (2 or more) that are literally stacked on top of each other.
	There are several types: 1) identical parcel polygons 2) overlapping parcels 3) parcels that share xy coordinates.
	Once the parcel records are identified, we select a "base" parcel that is associated with the "stacked" parcels (and building records) records
	and re-assign the PINS.
(HS) Thanks!
     How about the difference between pierce_improvement and pierce_improvement_builtas? They have a different number of records.
     And pierce_tax_account and tax_parcels_project?
(PMC) A: I think Mark addressed this, *current year should be used. I used the pierce_improvement since it looked more complete and it's what we used previously.
*/

-- ------------------------
-- Create indicies on relevant tables
-- ------------------------
alter table pierce_appraisal_account add index parcelnumber_index(parcelnumber(10));  /*(HS) not needed */
alter table pierce_improvement add index parcelnumber_index(parcelnumber(10));
alter table pierce_improvement add index building_id_index(buildingid);
alter table pierce_improvement_builtas add index parcelnumber_index(parcelnumber(10));
alter table pierce_improvement_builtas add index building_id_index(buildingid(4));
alter table pierce_improvement_detail add index parcelnumber_index(parcelnumber(10));
alter table pierce_improvement_detail add index building_id_index(buildingid(4));
alter table mobile_homes add index mh_parceln_index(mh_parceln);
alter table pierce_tax_account add index parcelnumber_index(parcelnumber(10));
alter table pierce_tax_account_description add index parcelnumber_index(parcelnumber(10));
alter table tax_parcels_project add index taxparceln_index(taxparceln);
-- Imported GIS processed tables
alter table base_parcel_dissolve add index taxparceln_index(taxparceln);
alter table base_parcel_dissolve_pt add index taxparceln_index(taxparceln);
alter table nonbase_parcel_dissolve add index taxparceln_index(taxparceln);
alter table nonbase_parcel_dissolve_pt add index taxparceln_index(taxparceln);
alter table nonbase_to_base add index taxparceln_index(taxparceln);
alter table nonbase_to_base add index taxparce_1_index(taxparce_1);

-- QC imported GIS processed tables and check for duplicates

-- Find duplicates for base_parcel_dissolve
drop table if exists tmp_duplicates_base_parcel_dissolve;
create temporary table tmp_duplicates_base_parcel_dissolve
select
	taxparceln,
	count(*) as a
from base_parcel_dissolve
group by
	taxparceln
having a > 1;

alter table tmp_duplicates_base_parcel_dissolve add index taxparceln_index(taxparceln);
-- Results of returned 0 duplicate records

-- Find duplicates for base_parcel_dissolve
drop table if exists tmp_duplicates_nonbase_parcel_dissolve;
create temporary table tmp_duplicates_nonbase_parcel_dissolve
select 
	taxparceln,
	count(*) as a
from nonbase_parcel_dissolve
group by
	taxparceln
having a > 1;

alter table tmp_duplicates_nonbase_parcel_dissolve add index taxparceln_index(taxparceln);
-- Results of query returned 0 duplicate records

-- Find duplicates for nonbase_to_base
drop table if exists tmp_duplicates_nonbase_to_base;
create temporary table tmp_duplicates_nonbase_to_base
select
	taxparceln,
	count(*) as a
from nonbase_to_base
group by
	taxparceln
having a > 1;

alter table tmp_duplicates_nonbase_to_base add index taxparceln_index(taxparceln);
-- Results of query returned 0 duplicate records

-- ------------------------
/* (HS) This comment seems to be (partly) out of date (?)*/
-- Prep tables and join kitfnl15 (polygon) and kitptfnl15 (point) together
-- Purpose is to ensure that the x,y values from the point file have been appended/joined to the polygon file for ease of use
-- QC/QA check - both tables should have same amount of record
-- ------------------------

drop table if exists prep_parcels_1;
create table prep_parcels_1
select
	a.taxparceln,
	b.taxparcelt,
	b.taxparcell,
	b.taxparcelu,
	b.taxable_va,
	a.point_x,
	a.point_y,
	a.area,
	b.site_addre,
	b.city_state,
	b.zipcode,
	b.land_acres,
	b.land_value,
	b.improvemen,
	b.use_code,
	b.landuse_de,
	b.exemption_
from base_parcel_dissolve_pt a
	inner join tax_parcels_project b on a.taxparceln = b.taxparceln
where
	b.taxparcelt in ('Base Parcel', 'Tax Purpose Only');
	
alter table prep_parcels_1 add index taxparceln_index(taxparceln);

/* (HS) prep_parcels_1 has one duplicate because the tax_parcels_project table has a duplicate:
   	taxparceln = 0119064049
	Maybe the tax_parcels_project table should be added to the set of tables above for which duplicates are checked?
	For now I just remove one record of those two records.
*/
delete from prep_parcels_1 where taxparceln = 0119064049 and taxable_va = 0;

/* (HS)	There are 19 records where improvement value is 0, but they have non zero value in pierce_tax_account.
   	Analogously for land value (12 records).
   	Can be filled as follows:
*/
update prep_parcels_1 a
       inner join  pierce_tax_account b on a.taxparceln = b.parcelnumber
       set a.improvemen = b.improvementvaluecurrentyear
       where a.improvemen = 0 and b.improvementvaluecurrentyear > 0;
update prep_parcels_1 a
       inner join  pierce_tax_account b on a.taxparceln = b.parcelnumber
       set a.land_value = b.landvaluecurrentyear
       where a.land_value = 0 and b.landvaluecurrentyear > 0;

/* (HS) Additional 284 records have zero current tax value in pierce_tax_account but non zero previous value in the same table.
   	For land value it's only 2 records.
   	Can be filled as follows, using 10 percent inflation:
*/
update prep_parcels_1 a
       inner join  pierce_tax_account b on a.taxparceln = b.parcelnumber
       set a.improvemen = 1.1*b.improvementvalueprioryear
       where a.improvemen = 0 and b.improvementvalueprioryear > 0;
update prep_parcels_1 a
       inner join  pierce_tax_account b on a.taxparceln = b.parcelnumber
       set a.land_value = 1.1*b.landvalueprioryearinteger
       where a.land_value = 0 and b.landvalueprioryearinteger > 0;

-- ---------------------------
-- Create parcels table
-- ---------------------------

drop table if exists parcels_bak;
create table parcels_bak select * from parcels;

drop table if exists parcels;
create table parcels
	(parcel_id int(11),
	parcel_id_fips varchar(30),
	parking_space_daily int(11),
	parking_space_hourly int(11),
	hschool_id int(11),
	city_id int(11),
	minority_id int(11),
	census_tract_id int(11),
	grid_id int(11),
	tod_id int(11),
	census_block_id int(11),
	y_coord_sp double,
	county_id int(11),
	plan_type_id int(11),
	-- use_code int(11),
	use_code varchar(30), /* (HS) this should be a string because of later join with a reclass table */
	transit_buffer_id int(11),
	school_district_id int(11),
	gross_sqft int(11),
	is_inside_urban_growth_boundary int(11),
	park_buffer_id int(11),
	x_coord_sp double,
	elem_id int(11),
	is_waterfront int(11),
	growth_center_id int(11),
	zip_id int(11),
	parking_price_hourly int(11),
	hex_id int(11),
	regional_geography_id int(11),
	large_area_id int(11),
	mschool_id int(11),
	land_value int(11),
	hct_block_id int(11),
	baseyear_built int(11),
	poverty_id int(11),
	zone_id int(11),
	tractcity_id int(11),
	faz_id int(11),
	growth_amenities_id int(11),
	is_in_transit_zone int(11),
	parking_price_daily int(11),
	subarea_id int(11),
	census_block_group_id int(11),
	faz_group_id int(11),
	land_use_type_id int(11),
	mix_split_id int(11),
	parcel_sqft int(11),
	uga_buffer_id int(11),
	exemption varchar(20),
	stacked_parcel_id varchar(50),
	stacked_parcel_flag int(11),
	original_parcel_id varchar(50),
	address varchar(100),
	tax_exempt_flag int(11) default 0 /* (HS) added; otherwise it fails on the tax exemption update below */ 
	);

insert into parcels
	(parcel_id_fips,
	land_value,
	use_code,
	exemption,
	parcel_sqft,
	y_coord_sp,
	x_coord_sp,
	address,
	zip_id)
select
	taxparceln as parcel_id_fips,
	land_value as land_value,
	use_code as use_code,
	exemption_ as exemption,
	area as parcel_sqft,
	point_y as y_coord_sp,
	point_x as x_coord_sp,
	site_addre as address,
	zipcode as zip_id
from prep_parcels_1;
-- returns some truncated errors in zip_code where some records have hyphenated 9-digit zips, just the first 5 pre-hyphen digits appear to be imported which is fine

alter table parcels add index parcel_id_index(parcel_id_fips(10));
alter table parcels add index use_code_index(use_code);

-- Tax Exemption flag
update parcels set tax_exempt_flag = 1 where exemption is not null;

-- ------------------------
-- Parcel clean up
-- ------------------------

update parcels set parcel_id = 0 where parcel_id is null;
update parcels set parcel_id_fips = 0 where parcel_id_fips is null;
update parcels set parking_space_daily = 0 where parking_space_daily is null;
update parcels set parking_space_hourly = 0 where parking_space_hourly is null;
update parcels set hschool_id = 0 where hschool_id is null;
update parcels set city_id = 0 where city_id is null;
update parcels set minority_id = 0 where minority_id is null;
update parcels set census_tract_id = 0 where census_tract_id is null;
update parcels set grid_id = 0 where grid_id is null;
update parcels set tod_id = 0 where tod_id is null;
update parcels set census_block_id = 0 where census_block_id is null;
update parcels set y_coord_sp = 0 where y_coord_sp is null;
update parcels set county_id = 0 where county_id is null;
update parcels set plan_type_id = 0 where plan_type_id is null;
update parcels set use_code = 0 where use_code is null;
update parcels set transit_buffer_id = 0 where transit_buffer_id is null;
update parcels set school_district_id = 0 where school_district_id is null;
update parcels set gross_sqft = 0 where gross_sqft is null;
update parcels set is_inside_urban_growth_boundary = 0 where is_inside_urban_growth_boundary is null;
update parcels set park_buffer_id = 0 where park_buffer_id is null;
update parcels set x_coord_sp = 0 where x_coord_sp is null;
update parcels set elem_id = 0 where elem_id is null;
update parcels set is_waterfront = 0 where is_waterfront is null;
update parcels set growth_center_id = 0 where growth_center_id is null;
update parcels set zip_id = 0 where zip_id is null;
update parcels set parking_price_hourly = 0 where parking_price_hourly is null;
update parcels set hex_id = 0 where hex_id is null;
update parcels set regional_geography_id = 0 where regional_geography_id is null;
update parcels set large_area_id = 0 where large_area_id is null;
update parcels set mschool_id = 0 where mschool_id is null;
update parcels set land_value = 0 where land_value is null;
update parcels set hct_block_id = 0 where hct_block_id is null;
update parcels set baseyear_built = 0 where baseyear_built is null;
update parcels set poverty_id = 0 where poverty_id is null;
update parcels set zone_id = 0 where zone_id is null;
update parcels set tractcity_id = 0 where tractcity_id is null;
update parcels set faz_id = 0 where faz_id is null;
update parcels set growth_amenities_id = 0 where growth_amenities_id is null;
update parcels set is_in_transit_zone = 0 where is_in_transit_zone is null;
update parcels set parking_price_daily = 0 where parking_price_daily is null;
update parcels set subarea_id = 0 where subarea_id is null;
update parcels set census_block_group_id = 0 where census_block_group_id is null;
update parcels set faz_group_id = 0 where faz_group_id is null;
update parcels set land_use_type_id = 0 where land_use_type_id is null;
update parcels set mix_split_id = 0 where mix_split_id is null;
update parcels set parcel_sqft = 0 where parcel_sqft is null;
update parcels set uga_buffer_id = 0 where uga_buffer_id is null;
update parcels set exemption = 0 where exemption is null;
update parcels set stacked_parcel_id = 0 where stacked_parcel_id is null;
update parcels set stacked_parcel_flag = 0 where stacked_parcel_flag is null;
update parcels set original_parcel_id = 0 where original_parcel_id is null;
update parcels set address = 0 where address is null;

/* (HS) Join with a reclass table that is up to date. What table should be used?
   	If joined with this 2014 reclass table, it fails unless use_code is defined as varchar above.
(PMC) A: psrc_2014_parcel_baseyear_working.land_use_generic_reclass is the reclass table used in the join and should have all parcel land use codes accounted for.

(HS) use_code 1501 (RV park) ends up with land_use_type_id NULL because it is NULL in the reclass table. A few other types have either 0 or NULL land_use_type_id.
I copied the pierce records from the 2014 reclass table into the Pierce DB and added land_use_type_id for code 1501 as 21 (recreation). Here is the code I used:

CREATE TABLE land_use_generic_reclass2
  (ID int(11) DEFAULT NULL,
  county int(11) DEFAULT NULL,
  count int(11) DEFAULT NULL,
  county_land_use_code varchar(50) DEFAULT NULL,
  land_use_description varchar(70) DEFAULT NULL,
  generic_land_use_1 varchar(50) DEFAULT NULL,
  generic_land_use_2 varchar(50) DEFAULT NULL,
  centers_land_use_report varchar(50) DEFAULT NULL,
  MIC_centers varchar(50) DEFAULT NULL,
  RGC_centers varchar(50) DEFAULT NULL,
  zoning mediumtext,
  generic_land_use_type_id int(11) DEFAULT NULL,
  land_use_type_id int(11) DEFAULT NULL
  );

insert into land_use_generic_reclass2
       select * from psrc_2014_parcel_baseyear_working.land_use_generic_reclass where county = 53;

update land_use_generic_reclass2 set land_use_type_id = 21 where county_land_use_code = "1501";

-- Peter added these modifications
-- SF residential
update land_use_generic_reclass2 set land_use_type_id = 24
       where county_land_use_code in ('1800', '9101', '9140', '9145', '9160');

-- Transfer development rights
update land_use_generic_reclass2 set land_use_type_id = 25
       where county_land_use_code in ('9190');

-- No Land Use Code
update land_use_generic_reclass2 set land_use_type_id = 17
       where county_land_use_code = '0';

-- The rest
update land_use_generic_reclass2 set land_use_type_id = 17
       where county_land_use_code = 'null';
 */

/* (HS) if desired, update counts in the reclass table */
update land_use_generic_reclass2 set count = NULL;
update land_use_generic_reclass2 a
       inner join (select use_code, count(*) as count from parcels group by use_code) as b
       	     on b.use_code = a.county_land_use_code
       set a.count = b.count;

/* (HS) Now joining with the updated reclass table instead of the 2014 table.
*/

update parcels a
	 inner join land_use_generic_reclass2 b on a.use_code = b.county_land_use_code
set
	a.land_use_type_id = b.land_use_type_id
where
	b.county = 53;

/* QC check for records not in land use generic reclass table
select count(*) from parcels a left join psrc_2014_parcel_baseyear_working.land_use_generic_reclass b on a.use_code = b.county_land_use_code where b.county_land_use_code is null and b.county = 53;
*/


-- ------------------------
-- Building assembly process
-- ------------------------

-- Create property_class/buiding use table
-- NOTE exported reclass table to access to insert generic buildings use 1 and 2 values (should only run this query 1 time)
/* QC check for records not in building use generic reclass table
select count(*) from pierce_improvement a left join psrc_2014_parcel_baseyear_working.building_use_generic_reclass b on a.primaryoccupancycode = b.county_building_use_code where b.county_building_use_code is null and b.county = 53;
*/

-- No need to create updated/new building_use_generic_reclass table; existing table contains all use codes for 2018
/* (HS) Maybe we should use here Mark's new table
(PMC) A: Yes, if Mark's revised table has recently added building use codes then it should be used in this process
*/

-- Create grouped/summarized improvement_builtas table since there are multiple building_id values per PIN (e.g. 9710000632)
drop table if exists improvement_builtas_grouped;

create temporary table improvement_builtas_grouped
select
	parcelnumber,
	buildingid,
	sum(builtassquarefeet) as sqft,
	sum(units) as units,
	sum(bedrooms) as bedrooms,
	min(yearbuilt) as year_built,
	count(*) as count
from pierce_improvement_builtas
group by
	parcelnumber,
	buildingid;

alter table improvement_builtas_grouped add index pin_bldg_id_index(parcelnumber(10), buildingid(10));

-- Create prep_buildings table
drop table if exists prep_buildings_1;
create table prep_buildings_1
select
	a.parcelnumber,
	a.buildingid,
	a.propertytype,
	a.squarefeet,
	a.netsquarefeet,
	a.primaryoccupancycode,
	a.primaryoccupancydescription,
	b.units,
	b.year_built,
	b.sqft as builtas_sqft,
	b.count as num_improvements
from pierce_improvement a
	left join improvement_builtas_grouped b on a.parcelnumber = b.parcelnumber and a.buildingid = b.buildingid;

alter table prep_buildings_1 add index parcelnumber_index(parcelnumber(10));
alter table prep_buildings_1 add index primaryoccuancycode_index(primaryoccupancycode(10));

-- Do I do anything for mobile home parks? Mobile home parks were already included in the pierce_improvement table

-- Calculate proportion improvement value for building records
alter table prep_buildings_1 add column proportion double;
alter table prep_buildings_1 add column improvement_value int(11);

drop table if exists tmp_bldgs_impvalue;
create temporary table tmp_bldgs_impvalue
select
	taxparceln as parcelnumber,
	sum(ifnull(improvemen,0)) as improvement_value
from prep_parcels_1
group by
	taxparceln;

alter table tmp_bldgs_impvalue add index parcelnumber_index(parcelnumber(10));

-- Create sum total building sqft by pin
drop table if exists tmp_bldgs_sqft;
create temporary table tmp_bldgs_sqft
select
	parcelnumber,
	count(*) as count,
	sum(squarefeet) as total_sqft
from prep_buildings_1
group by
	parcelnumber;

alter table tmp_bldgs_sqft add index parcelnumber_index(parcelnumber(10));

/* (HS) There seem to be a minimal difference between squarefeet and builtas_sqft (only 4 in total).
   	All buildings have sqft info! Wow! Thus, the improvement distribution by sqft OK.
 */

-- Calculate proportion
update prep_buildings_1 a
	inner join tmp_bldgs_sqft b on a.parcelnumber = b.parcelnumber
set a.proportion = a.squarefeet/b.total_sqft;

update prep_buildings_1 a
	inner join tmp_bldgs_impvalue b on a.parcelnumber = b.parcelnumber
set a.improvement_value = a.proportion * b.improvement_value;

-- Adjust units for non-residential type spaces
-- QC use code and generic use 1
/*drop table if exists tmp1;
create temporary table tmp1
select
	primaryoccupancycode,
	primaryoccupancydescription,
	generic_building_use_1,
	count(*) as count,
	sum(units) as units,
	sum(squarefeet) as sqft,
	sum(netsquarefeet) as netsqft
from prep_buildings_1 a
	inner join 2018_parcel_baseyear_working.building_use_generic_reclass_2018_v2 b on a.primaryoccupancycode = b.county_building_use_code
where
	county_id = 53
  -- and units > 0
group by
	primaryoccupancycode,
	primaryoccupancydescription,
	generic_building_use_1;

select * from tmp1;
*/
-- IMPORTANT!!!!!!!!!!!!!!!!
-- 7.26.19 I did not run this query yet, want to QC further
-- 7.30.19 I ran this query!
/* (HS) This query removes 150,660 DUs! 54090 warehousing, 32097 parking, 30544 commercial. What are those units?
   	I think such drastic step should be left to the imputation script.
	But I do realize that "units" might not always mean dwelling units, e.g. storage units, parking units etc.
   	Also, for joining with a building reclass table, Mark's revised table should be used now.
(PMC) A: Just as you assumed, the unit value was set = 0 because it could not be determined if these records are actual dwelling units
(HS) I suggest to zero out the units later when we split the records into residential and non-residential types. Thus, commenting this out.
*/
/*
update prep_buildings_1 a
	inner join 2018_parcel_baseyear_working.building_use_generic_reclass_2018_v2 b on a.primaryoccupancycode = b.county_building_use_code
set a.units = 0
where
	b.generic_building_use_1 not in ('Group Quarters', 'Mobile Home Park', 'Multi-Family Residential', 'Single Family Residential')
	and b.county = 53;
*/


/*
-- DO NOT RUN THESE QUERIES FOR ASSEMBLED - Run for ESTIMATED
-- Adjust residential units for apartment, condos, duplex, etc.

(HS) Couldn't we use the property type to impute missing units? For example, there are 46,242 records
     with propertytype "Mobile Home" and generic_building_use_1 being MF that have units=0.
(PMC) A: Yes, this is possible.

(HS) OK, going to use primaryoccupancydescription and building type.
     First we assign building_type_id
*/
alter table prep_buildings_1 add column building_type_id integer;
update prep_buildings_1 a left join
	(select * from 2018_parcel_baseyear_working.building_use_generic_reclass_2018_v2
		where county_id = 53) as b
       on a.primaryoccupancycode = b.county_building_use_code
       set a.building_type_id = b.building_type_id;

/* (HS) Check the missing records with
select propertytype, primaryoccupancycode, primaryoccupancydescription, building_type_id, count(*)
       from `prep_buildings_1`
where units = 0 and building_type_id in (19, 11, 12, 4)
group by propertytype, primaryoccupancycode, primaryoccupancydescription, building_type_id;
*/

/* (HS) Now do the assignment
*/
-- SF/MH
update prep_buildings_1
       set units = 1
where
	building_type_id in (19, 11) and units = 0;

update prep_buildings_1
       set units = 1
where
	building_type_id in (4,12) and
	primaryoccupancydescription in ('Condo - Separate Unit', 'Townhouse', 'Townhouse/Condo') and
	units = 0;

-- Duplex
update prep_buildings_1
       set units = 2
where
	building_type_id = 12 and
	primaryoccupancydescription in ('Duplex Conv', 'Duplex') and
	units = 0;

-- Triplex
update prep_buildings_1
       set units = 3
where
	building_type_id = 12 and
	primaryoccupancydescription = 'Triplex' and
	units = 0;

-- Units up to 19
update prep_buildings_1
       set units = 15
where
	building_type_id = 12 and
	primaryoccupancydescription = 'Apt Low Rise up to 19 Units' and
	units = 0;

-- Units up to 20-99
update prep_buildings_1
       set units = 60
where
	building_type_id = 12 and
	primaryoccupancydescription = 'Apt Low Rise 20 to 99 Units' and
	units = 0;

-- MHS late change to the 100+ - looks like a lot of these are one of multiple buildings on a parcel with this code that just happens to have a zero - IE 0420172038.
-- MHS make this instead units = builtas_sqft / 830 (usecode average where Units not zero), and round zeros up to 1 unit.

update prep_buildings_1
       set units = round(builtas_sqft/830,0)
where
	building_type_id = 12 and
	primaryoccupancydescription = 'Apt Low Rise 100 Units Plus' and
	units = 0;

update prep_buildings_1
       set units = 1
where
	building_type_id = 12 and
	primaryoccupancydescription = 'Apt Low Rise 100 Units Plus' and
	units = 0;

-- MHS Original 100+ imputation code
-- Units 100+
/*update prep_buildings_1
       set units = 120
where
	building_type_id = 12 and
	primaryoccupancydescription = 'Apt Low Rise 100 Units Plus' and
	units = 0;
*/
-- Create backup of "stacked/overlapping" shapefile
create table nonbase_to_base_bak select * from nonbase_to_base;

/* (HS) In order to run the following queries, one needs to start
   	from a fresh clean nonbase_to_base table.
	Peter says its the nonbase_to_base_bak.
	Copy it with this query:
	drop table nonbase_to_base;
	create table nonbase_to_base select *
		       from pierce_2018_parcel_baseyear.nonbase_to_base_bak;
	alter table nonbase_to_base add index taxparceln_index(taxparceln);
	alter table nonbase_to_base add index taxparce_1_index(taxparce_1);
*/
-- Discovered land and improvement value = 0 for several records, but tax account has > 0 values
alter table nonbase_to_base add column land_value_current_year int(11);
alter table nonbase_to_base add column improvement_value_current_year int(11);

/* (HS) Wen asking Peter why are we using the pierce_tax_account table
why above we used tax_parcel_project, he says:
The tax_account file is used because it has data that is missing
from the tax_parcels_project file. I believe earlier in the script
I use the tax_parcel_project file where the data is available and
then update missing values using the tax_account.
*/
update nonbase_to_base a
	inner join pierce_tax_account b on a.taxparceln = b.parcelnumber
set
	a.land_value_current_year = b.landvaluecurrentyear,
	a.improvement_value_current_year = b.improvementvaluecurrentyear;

/* (HS) There are no missing land values that would have a non-zero value in prior year,
   	but there are a few for improvement value. Thus, we'll use it.
*/
update nonbase_to_base a
	inner join pierce_tax_account b on a.taxparceln = b.parcelnumber
set
	a.improvement_value_current_year = 1.1*b.improvementvalueprioryear
where a.improvement_value_current_year = 0 and b.improvementvalueprioryear > 0;

/* (HS) Fill in missing values using the tax_parcels_project table
*/
update nonbase_to_base a
	inner join tax_parcels_project b on a.taxparceln = b.taxparceln
set
	a.improvement_value_current_year = b.improvemen
where a.improvement_value_current_year = 0 and b.improvemen > 0;



-- Find records from "stacked/overlapping" shapefile that already exists in base dissolved parcel shapefile
drop table if exists prep_stacked_buildings_already_exists_tobe_deleted_1;
create table prep_stacked_buildings_already_exists_tobe_deleted_1
select
	a.*
from nonbase_to_base a
	inner join base_parcel_dissolve b on a.taxparceln = b.taxparceln;

alter table prep_stacked_buildings_already_exists_tobe_deleted_1 add index taxparceln_index(taxparceln);

-- (HS) 29 rows will be deleted.
delete a.*
from nonbase_to_base a
	inner join base_parcel_dissolve b on a.taxparceln = b.taxparceln;

-- Begin assembling the stacked parcels and associating it with the processed prep_buildings_1 table
drop table if exists prep_stacked_buildings_1;
create table prep_stacked_buildings_1
select
	a.taxparceln as stacked_pin,
	a.taxparce_1 as base_parcel,
	a.land_value_current_year as land_value_orig,
	a.improvement_value_current_year as improvement_value_orig,
	a.point_x as x_coord,
	a.point_y as y_coord,
	b.*
from nonbase_to_base a
	left join prep_buildings_1 b on a.taxparceln = b.parcelnumber;

alter table prep_stacked_buildings_1 add index stacked_pin_index(stacked_pin);
alter table prep_stacked_buildings_1 add index building_id_index(buildingid);

-- Update improvement value
update prep_stacked_buildings_1
set improvement_value = (improvement_value_orig * proportion)
where improvement_value is null;


-- Check if base_parcel is found in base polygon shapefile - 294 records did not match because base_parcel is empty set.
/* (HS) I don't understand the comment above - the select command results in 14700 and base_parcel_dissolve is not empty. */
/* (PMC) A: Don't worry about this query it was just a QC on my end*/
select
	count(*)
from prep_stacked_buildings_1 a
	inner join base_parcel_dissolve b on a.base_parcel = b.taxparceln;

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
	use_code text,
-- 	building_type_id int(11), /*(HS) added */  MHS Not needed already in list 6 rows earlier
	stacked_pin text);

-- Insert residential portion of prep_buildings_1 into buildings table
/* (HS) Can we be sure that the primaryoccupancycode is up to date and is listing all residential uses?
   	Might be missing: 321 (dormitory), 862 (condo), 881 (RV park)
	Or can this be done by joining with the reclass table like above when zeroing our DUs for non-res uses?
(PMC) A: I'll check Mark's updated building reclass table. Updated (5/6/2020) Mark did include these use codes listed above.
(HS) Peter put in the missing codes, so it's good now.
*/
insert into buildings
	(building_id,
	gross_sqft,
	sqft_per_unit,
	year_built,
	parcel_id,
	residential_units,
	improvement_value,
	use_code,
	building_type_id) /*(HS) added */
select
	buildingid as building_id,
	builtas_sqft as gross_sqft,
	(squarefeet/units) as sqft_per_unit,
	year_built as year_built,
	parcelnumber as parcel_id,
	units as residential_units,
	improvement_value,
	primaryoccupancycode as use_code,
	building_type_id  /*(HS) added */
from prep_buildings_1
where
	primaryoccupancycode in (26, 30, 97, 100, 101, 102, 103, 106, 107, 108, 109, 110, 114, 119, 128, 129, 131, 298, 300, 321, 323, 352, 521, 587, 850, 851, 852, 861, 862, 863, 864, 865, 880, 881, 1312);

-- Insert non-residential buildings into buildings table
/* (HS) Here we will zero out units (a step skipped earlier in the script)
*/
insert into buildings
	(building_id,
	gross_sqft,
	non_residential_sqft,
	year_built,
	parcel_id,
	residential_units, /*(HS) added */
	improvement_value,
	use_code,
	building_type_id) /*(HS) added */
select
	buildingid as building_id,
	builtas_sqft as gross_sqft,
	squarefeet as non_residential_sqft,
	year_built as year_built,
	parcelnumber as parcel_id,
	0, /*(HS) added */
	improvement_value,
	primaryoccupancycode as use_code,
	building_type_id /*(HS) added */
from prep_buildings_1
where
	primaryoccupancycode not in (26, 30, 97, 100, 101, 102, 103, 106, 107, 108, 109, 110, 114, 119, 128, 129, 131, 298, 300, 321, 323, 352, 521, 587, 850, 851, 852, 861, 862, 863, 864, 865, 880, 881, 1312);

alter table buildings add index parcel_id_index(parcel_id(10));
alter table buildings add index building_id_index(building_id);
alter table buildings add index use_code_index(use_code(10));

/* (HS) What happens with mix-use buildings? There are none? */
/* (PMC) A: Pierce County does not have mixed-used building types. */

-- Note the prep_buildings_1 table contains 343,065 records. The resulting buildings table contains 340,612 records. There are 2453 records that do not contain a primaryoccupancycode value

/* (HS) These missing records should be added! The imputation procedure can then impute the building type based on the land use type.
   	However, if the propertytype is not "Residential", the units should be zeroed out, since we don't want warehouses and parking units
	to be considered as DUs.

(PMC) A: I am not opposed to adding these records back into the table if the imputation script can provide a suitable use code value. I am ok with zeroing the unit value for these records if it is not residential type.
*/
/* (HS) OK, here is the insertion of the missing records, residential and non-residential
*/
insert into buildings
	(building_id,
	gross_sqft,
	sqft_per_unit,
	year_built,
	parcel_id,
	residential_units,
	improvement_value,
	use_code,
	building_type_id)
select
	buildingid as building_id,
	builtas_sqft as gross_sqft,
	(squarefeet/units) as sqft_per_unit,
	year_built as year_built,
	parcelnumber as parcel_id,
	units as residential_units,
	improvement_value,
	primaryoccupancycode as use_code,
	building_type_id
from prep_buildings_1
where
	primaryoccupancycode  is null and propertytype = "Residential";


insert into buildings
	(building_id,
	gross_sqft,
	non_residential_sqft,
	year_built,
	parcel_id,
	residential_units,
	improvement_value,
	use_code,
	building_type_id)
select
	buildingid as building_id,
	builtas_sqft as gross_sqft,
	squarefeet as non_residential_sqft,
	year_built as year_built,
	parcelnumber as parcel_id,
	0,
	improvement_value,
	primaryoccupancycode as use_code,
	building_type_id
from prep_buildings_1
where
	primaryoccupancycode  is null and propertytype <> "Residential";


-- Update "stacked/overlapping" records' improvement_value in buildings table
update buildings a
	inner join prep_stacked_buildings_1 b on a.parcel_id = b.stacked_pin and a.building_id = b.buildingid
set a.improvement_value = b.improvement_value;

-- Update "stacked/overlapping" records stacked_pin field
	-- it's not updating 422 records... why?
update buildings a
	inner join prep_stacked_buildings_1 b on a.parcel_id = b.stacked_pin and a.building_id = b.buildingid
set a.stacked_pin = b.stacked_pin;

	-- Diagnose 512 records
	/*
	drop table if exists tmp_missing_stacked_records;
	create temporary table tmp_missing_stacked_records
	select
		a.*
	from prep_stacked_buildings_1 a
		left join buildings b on a.stacked_pin = b.parcel_id and a.buildingid = b.building_id
	where
		b.parcel_id is null and b.building_id is null;

	alter table tmp_missing_stacked_records add index stacked_pin_index(stacked_pin);
	alter table tmp_missing_stacked_records add index buildingid_index(buildingid);

	-- 373 records have buildingid is null

	Discovered that PIN value in the table is no so there are no improvement records for these stacked pins

	*/
	/* (HS) Do I understand it right: the nobase_to_base table contains parcel records that are not present in pierce_improvement(_builtas).
	   	That's why we get nulls on buildingsid?
	(PMC) A: Yes there will be inconsistencies in the nobase_to_base (a version of the parcels table) to the pierce_improvements (_builtas) because not all parcels will have buildings and also because of general inconsistencies in the data.
	*/

update buildings a
	inner join prep_stacked_buildings_1 b on a.parcel_id = b.stacked_pin and a.building_id = b.buildingid
set a.parcel_id = b.base_parcel;

/* (HS) Why is the code below commented out? */
/* (PMC) A: I believe I wasn't entirely sure this should be applied since I QC'd some records and the land value was extremely high. I am still not certain we should apply this fix; besides it only effects 3,000 or so records. I will QC it a bit more but it should be skipped for now.
(HS) I don't see that the land value in nonbase_to_base is too high (max is about 8 million).
     Could it be that you get high values because you are summing the land value from buildings,
     but the land value for each building is the total land value of the parcel.
     So these should not be summed.
(See Peter's answer below.)
*/
/*
-- The base parcels for some records are missing land value, use "stacked/overlapping" records to calculate base land value if it doesn't exist
	-- remember the land_value_orig from prep_stacked_buildings_1 originated from the tax_account table

drop table if exists prep_stacked_buildings_land_value_1;
create table prep_stacked_buildings_land_value_1
select
	-- stacked_pin,
	base_parcel,
	count(*) as cnt,
	sum(improvement_value_orig) as improvement_value,
	land_value_orig as land_value
	-- sum(land_value_orig) as land_value
from prep_stacked_buildings_1
where
	base_parcel <> ""
group by
	-- stacked_pin,
	base_parcel;

-- alter table prep_stacked_buildings_land_value_1 add index stacked_pin_index(stacked_pin);
alter table prep_stacked_buildings_land_value_1 add index based_parcel_index(base_parcel);

update parcels a
	inner join prep_stacked_buildings_land_value_1 b on a.parcel_id = b.base_parcel
set a.land_value = b.land_value
where a.land_value = 0;
*/
/* (HS) I think the query should be like this, but there is soomething wrong with the "on" columns.
   Peter says:
   So this one is tricky, there are parcels that share the same PIN but have different land values
   and some that do not have different land values, which makes it difficult to aggregate or not aggregate
   the land value. I suggest we leave this commented out. I realize that this will leave records
   where the land value = 0, but it's a small number of records.
   In the meantime 'll try and find a better fix for this.
*/
/*update parcels a
	inner join nonbase_to_base b on a.parcel_id = b.taxparce_1
set a.land_value = b.land_value_current_year
where a.land_value = 0;
*/

-- ------------------------
-- Clean up buildings table fields
-- ------------------------
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
set stacked_pin = 0
where stacked_pin is null;

/* (HS) Join with Mark's new reclass table
   	Actually, we don't need it anymore, since we already assigned building_type_id.
*/

/*update buildings a
	inner join psrc_2014_parcel_baseyear_working.building_use_generic_reclass b on a.use_code = b.county_building_use_code
set
	a.building_type_id = b.building_type_id
where
	b.county = 53;
*/

/* -- MHS QC check
select building_type_id, use_code, count(*) as records, sum(residential_units) as du, sum(non_residential_sqft) as sqft, sum(gross_sqft) as gross_sqft
from buildings
group by building_type_id, use_code;
*/
-- Add county id code to buildings table
alter table buildings add column county_id int(11);

update buildings set county_id = 53;

alter table buildings add index county_id_index(county_id);

-- ------------------------
-- Delete records from buildings table that do not exists in parcels table
-- ------------------------
drop table if exists buildings_before_delete_not_in_parcels;

create table buildings_before_delete_not_in_parcels
select *
from buildings;

delete a.*
from buildings a left join parcels b on a.parcel_id = b.parcel_id_fips
where b.parcel_id_fips is null;

-- -------
--  MHS Late QC changes - removing 34,000+ records use_code = 99, "Detached Garage" that were in Outbuildings up to now.
-- -------
delete from buildings where use_code = 99;

-- -------
--  MHS Converting building_id to a unique number, porting over the orignal values to a new column
-- -------

alter table buildings add column building_id_orig int(11);
update buildings set building_id_orig = 0;
update buildings set building_id_orig = building_id;
update buildings set building_id = 0;
alter table buildings change column building_id building_id int(11) auto_increment primary key;

-- ------------------------
-- Delete records from parcels table that do not exists in the parcel shapefile
-- ------------------------
drop table if exists parcels_before_delete_not_in_gis;

create table parcels_before_delete_not_in_gis
select *
from parcels;

delete a.*
from parcels a left join base_parcel_dissolve b on a.parcel_id_fips = b.taxparceln
where b.taxparceln is null;


/* (HS) 14522 records deleted! */
