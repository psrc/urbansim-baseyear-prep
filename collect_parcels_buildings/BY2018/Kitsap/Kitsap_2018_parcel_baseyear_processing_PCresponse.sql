/*
Date: 02/5/2020
Author: Peter Caballero
Reviewer: Hana Sevcikova (notes below marked by HS).
Review Date: 04/14/2020
Purpose: Assemble 2018 Kitsap County Parcels and Buildings tables
*/

/*--------------------------
(HS)
Taking a database already pre-populated with the following tables: 
buildings_assessor
flatats
land
main
parcels_gis
prcl
kitsap_commercial_improvement
kitsap_dwelling
kitsap_mobile_home
kitsap_parcel
kitsap_valuation
(HS) Q: where do they come from? Need to be documented. See below for not needed tables.
(PMC) A: These files were downloaded from the Kitsap County Assessor's website. I *think* I created a comprehensive meta file for all counties downloaded extracts. I'll try and find it.
*/

/*--------------------------
# Create indicies on relevant tables
(HS) not tested since the tables already have indices
*/--------------------------

alter table buildings_assessor add index rp_acct_id_index(rp_acct_id);
alter table buildings add index use_desc_index(use_desc); /* (HS) This table should not be there, since it is created in this script, right? */ /* (PMC) A: If I recall the raw Kitsap County assessor file has a table named "buildings". */
alter table flatats add index rp_acct_id_index(rp_acct_id);
alter table land add index rp_acct_id_index(rp_acct_id);
alter table land add index prop_class_index(prop_class);
alter table main add index rp_acct_id_index(rp_acct_id);
alter table parcels_gis add index rp_acct_id_index(rp_acct_id); /* (HS) How is this table used? */ /*(PMC) A: I believe this is Kitsap County's attribute table from its shapefile; it is named "parcels" from the assessor so I renamed it. */
alter table parcels_gis add index pid_index(pid);
alter table prcl add index rp_acct_id_index(rp_acct_id);
alter table prclpt add index rp_acct_id_index(rp_acct_id); /* (HS) Table is not used in this script */ /* (PMC) A: This is another GIS processed attribute table; the point shapefile version of the parcel polygon file  */
alter table kitsap_commercial_improvement add index rp_acct_id_index(rp_acct_id); /* (HS) How is this table used? */ /* (PMC) A: Not used - I believe it was downloaded from the assessor's site, but the raw "buildings" table contained most if not all of the commercial data. */ 
alter table kitsap_dwelling add index rp_acct_id_index(rp_acct_id); /* (HS) How is this used? */ /* (PMC) A: Not used - Similar to the "kitsap_commerical_improvement" table this table was used to id residential buildings, but the raw "buildings" table contained most of the residential data. */
alter table kitsap_mobile_home add index rp_acct_id_index(rp_acct_id);
alter table kitsap_parcel add index rp_acct_id_index(rp_acct_id); /* (HS) How is this table used? */ /* (PMC) A: Not used - since the prcl or parcels_gis table contained the correct attribute data. */
alter table kitsap_valuation add index rp_acct_id_index(rp_acct_id);

/*--------------------------
# Combine info from prcl, flatats, land and main to create prep_parcels_1
*/--------------------------

drop table if exists prep_parcels_1;
create table prep_parcels_1
select
	a.*,
	c.levy_code,
	c.jurisdict,
	c.acres,
	c.bldg_value,
	c.land_value,
	c.assd_value,
	d.acres as acres_land,
	d.nbrhd_cd,
	d.prop_class,
	d.zone_code,
	d.num_dwell,
	d.num_other,
	d.tot_improv,
	e.acct_stat,
	e.tax_status,
	e.tax_year
from prcl a
	left join flatats c on a.rp_acct_id = c.rp_acct_id
	left join land d on a.rp_acct_id = d.rp_acct_id
	left join main e on a.rp_acct_id = e.rp_acct_id;

-- Remove records where the identifier is zero
-- (HS) this seems to be an extra record in prcl not present in the other tables
/* select count(*) from prcl a 
	left join prep_parcels_1 b on a.rp_acct_id = b.rp_acct_id where b.rp_acct_id is null;*/
delete a.* from prep_parcels_1 a where a.rp_acct_id = 0;

-- Create an index
alter table prep_parcels_1 add index rp_acct_id_index(rp_acct_id);

/*
There are 4 extra records in the prep_parcels_1 table
mysql> select rp_acct_id, count(*) a from prep_parcels_1 group by rp_acct_id having a > 1;
+------------+---+
| rp_acct_id | a |
+------------+---+
|    1149301 | 2 |
|    1443571 | 2 |
|    1842178 | 2 |
|    2266096 | 2 |
+------------+---+ 	 	
(HS) These are duplicates in the flatats table.
(HS) Q: There are many records that are in the other tables but not in the prcl table. Is it OK to leave them out?
(PMC) A: Yes, I believe the flatats file is just subdivided parcels, but carries no useable attribute data so we can't use it. 
     	Comparison with flatats gives almost 8500 rows:
select * from flatats a 
	left join prcl b on a.rp_acct_id = b.rp_acct_id where b.rp_acct_id is null; 
*/


/*-----------------------------
# Create parcels table
*/-----------------------------
drop table if exists parcels_bak;
create table parcels_bak select * from parcels;

drop table if exists parcels;
create table parcels
	(parcel_id int(11),
	-- parcel_id_fips varchar(30),
	parcel_id_fips int(11), /* (HS) should be int because of buildings.parcel_id will be int */ /* (PMC) A: the raw assessor's parcel_id is text, so this property type needs to be retained. A new PSRC parcel_id is created when the tables are collated. */
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
	use_code varchar(30), /* (HS) this should be a string because of later join with a reclass table */ /* (PMC) A: the raw assessor's use_code (property class) is int, so this needs to be retained. */ 
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
	address varchar(100));

alter table parcels add index parcel_id_index(parcel_id);
alter table parcels add index parcel_id_fips_index(parcel_id_fips);
alter table parcels add index use_code_index(use_code);

-- (HS) Note that below the rp_acct_id columns becomes parcel_id_fips, while parcel_id is not used.
insert into parcels 
	(parcel_id_fips,
	land_value,
	use_code,
	exemption,
	gross_sqft,
	y_coord_sp,
	x_coord_sp)
select
	rp_acct_id as parcel_id_fips,
	land_value as land_value,
	prop_class as use_code,
	tax_status as exemption,
	poly_area as gross_sqft,
	centroid_y as y_coord_sp,
	centroid_x as x_coord_sp
from prep_parcels_1;


--------------------------
-- Parcel clean up
-- (HS) All columns that were not filled above are NULLs, thus need to be set to 0 here.
--------------------------
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

/*
(HS) Is this reclass table still up to date? 
     It results in 3170 records of having NULL land use. 
     Their use code is 0 and 999.
     
(PMC) A: No this is not up-to-date
*/

update parcels a
        inner join psrc_2014_parcel_baseyear_working.land_use_generic_reclass b on a.use_code = b.county_land_use_code
set
	a.land_use_type_id = b.land_use_type_id
where
	b.county = 35;

/*
(HS) So far, the parcels table does not have parcel_sqft. Where should the info come from?
     Maybe parcels_gis.Shape_Area? 
     Why there is a different # rows in parcels_gis and parcels?
     
(PMC) A: Correct, the parcel_sqft is computed is GIS. I believe the difference is that some polygon records do not have parcel attribute date. 
*/


/*--------------------------
# Building assembly process
*/--------------------------


-- Create prep_buildings table from the assessor table

drop table if exists prep_buildings;
create table prep_buildings
	(id int auto_increment, primary key (id),
	improv_type varchar(10),
	rp_acct_id int(11),
	total_sqft int(11),
	year_built smallint(6),
	stories smallint(6),
	bedrooms smallint(6),
	residential_units smallint(6),
	bldg_type varchar(40),
	use_desc varchar(30),
	proportion double,
	improvement_value int(11),
	property_class varchar(40));

insert into prep_buildings
	(improv_type,
	rp_acct_id,
	total_sqft,
	year_built,
	stories,
	bedrooms,
	bldg_type,
	use_desc)
select
	improv_typ as improv_type,
	rp_acct_id,
	flr_tot_sf as total_sqft,
	year_built,
	stories,
	bedrooms,
	bldg_typ as bldg_type,
	use_desc
from buildings_assessor;
	
-- Append Mobile Home records to prep_buildings
insert into prep_buildings 
	(rp_acct_id,
	total_sqft,
	use_desc)
select
	realpropertyaccountid as rp_acct_id,
	mobilehomesquarefeet as total_sqft,
	'mobile_home' as use_desc
from kitsap_mobile_home;

alter table prep_buildings add index rp_acct_id_index(rp_acct_id);

update prep_buildings
set 
	residential_units = 0,
	proportion = 0,
	improvement_value = 0;


-- Calculate proportion improvement value for building records
/* 
(HS) Below, 24737 records (more than 1/4 of all records) have 0 improvement value.   
     827 of those could be filled if we use taxyear 2019 (possibly with some adjustments?)
     
(PMC) A: I am not 100% opposed to that option, but that opens a rabbit hole of using 2019 data to backfill other missing data as well?
*/
drop table if exists tmp_bldgs_impvalue;
create temporary table tmp_bldgs_impvalue
select
	realpropertyaccountid as rp_acct_id, 
	sum(ifnull(improvementmarketvalue,0)) as improvement_value
from kitsap_valuation
where 
	taxyear = 2018
group by
	realpropertyaccountid;

alter table tmp_bldgs_impvalue add index rp_acct_id_index(rp_acct_id);

-- Create sum total building sqft by rp_acct_id
drop table if exists tmp_bldgs_sqft;
create temporary table tmp_bldgs_sqft
select
	rp_acct_id,
	count(*) as count,
	sum(total_sqft) as total_sqft
from prep_buildings
group by
	rp_acct_id;

alter table tmp_bldgs_sqft add index rp_acct_id_index(rp_acct_id);

-- Calculate proportion
update prep_buildings a
	inner join tmp_bldgs_sqft b on a.rp_acct_id = b.rp_acct_id
set a.proportion = a.total_sqft/b.total_sqft;

/* 
(HS) Here we are leaving 1650 records of improvement value out because of missing total_sqft.
     In such cases, can we just set it to total improvement value / number of buildings?
*/
update prep_buildings a 
	inner join tmp_bldgs_impvalue b on a.rp_acct_id = b.rp_acct_id
set a.improvement_value = a.proportion * b.improvement_value;


-- SF/MH
/*
(HS) We do this because there is no info on DUs otherwise, right? Is it just a Kitsap issue?
(PMC) A: Correct, I believe Snohomish and/or Pierce have a similar issue
*/
update prep_buildings
set residential_units = 1
where
	use_desc in ('Single Family', 'Single family - Owner', 'mobile_home');

-- Duplex
update prep_buildings
set residential_units = 2
where 
	use_desc in ('Duplex');

-- Triplex
update prep_buildings
set residential_units = 3
where 
	use_desc in ('Triplex');

-- Fiveplex
update prep_buildings
set residential_units = 5
where 
	use_desc in ('5');


-- STOPPED WORKING 9.16.2019

/* Did not run these queries because I cannot identify how many units to impute	
-- Units 5-9
update prep_buildings
set residential_units = 4
where 
	property_class = 131
	and imp_description = 'apart'
	and residential_units is null;

-- Units 10-14
update prep_buildings_1
set residential_units = 14
where 
	property_class = 132
	and imp_description = 'apart'
	and residential_units is null;

-- Units 15-19
update prep_buildings_1
set residential_units = 19
where 
	property_class = 133
	and imp_description = 'apart'
	and residential_units is null;

-- Units 20-29
update prep_buildings_1
set residential_units = 29
where 
	property_class = 134
	and imp_description = 'apart'
	and residential_units is null;

-- Units 30-39
update prep_buildings_1
set residential_units = 39
where 
	property_class = 135
	and imp_description = 'apart'
	and residential_units is null;

-- Units 40-49
update prep_buildings_1
set residential_units = 49
where 
	property_class = 136
	and imp_description = 'apart'
	and residential_units is null;

-- Units 40-49
update prep_buildings_1
set residential_units = 50
where 
	property_class = 137
	and imp_description = 'apart'
	and residential_units is null;
*/

/*
(HS):
There are 245 records with missing residential units. I suggest to impute it as round(# bedrooms / 2)
But the maxim # bedrooms is 46 for a 4-6MF, which means 23 units :-(
9 records do not have # bedrooms, so the 4-6MF could be imputed as 4 DUs and the rest 
could be imputed later in the R script.
*/

-- Create buildings table
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
	/* parcel_id varchar(20), (HS) this sould be int */ /* (PMC) A: Will retain the raw assessor use code, a new parcel_id will be created when the tables are collated. */
	parcel_id int(11),
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
	stacked_pin varchar(20));

-- Insert residential buildings into buildings table
insert into buildings
	(building_id,
	gross_sqft,
	non_residential_sqft,
	year_built,
	parcel_id,
	residential_units,
	improvement_value,
	use_code)
select
	id as building_id,
	total_sqft as gross_sqft,
	total_sqft as non_residential_sqft, /* (HS) why are we setting total_sqft as nonres_sqft also for residential buildings? */ 
	year_built as year_built,
	rp_acct_id as parcel_id,
	residential_units as residential_units,
	improvement_value,
	use_desc as use_code
from prep_buildings;

alter table buildings add index parcel_id_index(parcel_id);
-- --------------------------
-- Clean up buildings table fields
-- --------------------------
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

/*
(HS) Q: Should this be now taken from the reclass table that Mark revised?
(PMC) A: Yes, Mark's table is the latest/greatest reclass table. 
*/
update buildings a 
	inner join psrc_2014_parcel_baseyear_working.building_use_generic_reclass b on a.use_code = b.county_building_use_code
set
	a.building_type_id = b.building_type_id
where
	b.county = 35;

-- Delete records from buildings table that do not exists in parcels table
/* (HS) Q: What are those buildings? They amount to 9655 DUs.
(PMC) A: Unfortunately, we cannot retain these records because they are not linked to any parcel_id in the GIS file (no spatial representation)
*/
drop table if exists buildings_before_delete_not_in_parcels;
create table buildings_before_delete_not_in_parcels
select *
from buildings;

delete a.*
from buildings a left join parcels b on a.parcel_id = b.parcel_id_fips
where b.parcel_id_fips is null;
	
-- Delete records from parcels table that do not exists in the parcel shapefile (aka kitptfnl15)
/* (HS) 
This step seems unnecessary, since the parcels table was created from the prcl table.
Thus, no parcels are removed. But if they were any, one would need to run the 
previous step after this one, because it can also exclude buildings on those deleted parcels.

(PMC) A: Step was done since it was consistent with other counties
*/ 
drop table if exists parcels_before_delete_not_in_gis;
create table parcels_before_delete_not_in_gis
select *
from parcels;

delete a.*
from parcels a left join prcl b on a.parcel_id = b.rp_acct_id
where b.rp_acct_id is null;

-- Create county_id field on buildings table
alter table buildings add column county_id int(11);

update buildings
set county_id = 35;

alter table buildings add index county_id_index(county_id);

-- FINISHED 2018 Assembly Process --
/* (HS) Q: Is any stuff below relevant for 2018?
(PMC) A: Not necessary and can be deleted. 
*/



/*
-- Improvement Value for residential buildings 
-- Must run after the parcels table has been assembled
-- Create split of improvement value to individual building records; all improvement value is aggregated at a single parcel number record
-- NOTE - Improvement value extremely low for selected records 5.23.2012
alter table prep_res_buildings add column proportion double;
alter table prep_res_buildings add column total_bldg_sqft double;

drop table if exists proportion_building;
create table proportion_building
select
	rp_acct_id,
	sum(flr_tot_sf) as total_bldg_sqft,
	count(*) as count
from sub_buildings
group by 
	rp_acct_id
having count > 1;

alter table proportion_building add index rp_acct_id_index(rp_acct_id);

update sub_buildings a inner join proportion_building b on a.rp_acct_id =b.rp_acct_id
set a.total_bldg_sqft = b.total_bldg_sqft;

update sub_buildings set proportion = flr_tot_sf/total_bldg_sqft;

update sub_buildings set proportion = 1 where total_bldg_sqft is null;

# Add improvment value column and update data
alter table sub_buildings add column improvement_value int(11);

update sub_buildings a inner join parcels b on a.rp_acct_id = b.parcel_id
set a.improvement_value = (a.proportion * b.improvement_value);
*/
# IMPORTANT - DO NOT RUN FOR LOCAL TARGETS REPRESENTATION - INSTEAD USE MARK S. PROCESS
/*
# PMC imputing Residential Units Process
# Use the proportion_buildings table created for the improvement allocation process to distribute residential units


# SF units
update sub_buildings a inner join building_use_generic_reclass b on a.use_desc = b.description 
set residential_units = 1
where b.generic_building_use_1 = 'Single Family Residential';

# Duplex units
update sub_buildings 
set residential_units = 2
where use_desc = 'Duplex';

# Triplex units
update sub_buildings 
set residential_units = 3
where use_desc = 'Triplex';
*/

# IMPUTATION PROCESS USED FOR LOCAL TARGEST REPRESENTATION
# Update residential units to sub_buildings table via Mark S. imputed units file (see Mark S for imputation method and data)
# Mark S imputed Census Tract residential units
# Imported tables from: J:\Projects\UrbanSim\NEW_DIRECTORY\Databases\Access\Parcels\Kitsap\2011\MHS_Final_Kitsap_Est_Units_Tables.accdb

alter table mhs_copy_kitsap_building_dbf add index rp_acct_id_index(rp_acct_id);
alter table pmc_table_1_apartment_du_by_rp_accnt_num add index rp_acct_id_index(rp_acct_id);
alter table pmc_table_2_mobile_home_du_by_rp_accnt_num add index rp_acct_id_index(rp_acct_id);

alter table sub_buildings add column proportion_residential double;
alter table sub_buildings add column total_residential_bldg_sqft double; 

# Create proportion of residenital buildings to apply imputed residential units
drop table if exists proportion_residential_building;
create table proportion_residential_building
select
	rp_acct_id,
	sum(flr_tot_sf) as total_bldg_sqft,
	count(*) as count
from sub_buildings a inner join building_use_generic_reclass b on a.use_desc = b.description
where b.generic_building_use_2 = 'R'
group by 
	rp_acct_id
having count > 1;

alter table proportion_residential_building add index rp_acct_id_index(rp_acct_id);

update sub_buildings a inner join proportion_residential_building b on a.rp_acct_id =b.rp_acct_id
set a.total_residential_bldg_sqft = b.total_bldg_sqft;

update sub_buildings set proportion_residential = flr_tot_sf/total_residential_bldg_sqft;

update sub_buildings set proportion_residential = 1 where total_residential_bldg_sqft is null;

# Single Family Units
/*update sub_buildings a inner join mhs_copy_kitsap_building_dbf b on a.rp_acct_id = b.rp_acct_id
	inner join building_use_generic_reclass c on a.use_desc = c.description
set a.residential_units = (a.proportion * b.sf_du)
where c.generic_building_use_2 = 'R';*/

update sub_buildings a inner join mhs_copy_kitsap_building_dbf b on a.rp_acct_id = b.rp_acct_id
set a.residential_units = b.sf_du;

# Apartment Units
update sub_buildings a inner join PMC_Table_1_Apartment_DU_by_RP_Accnt_Num b on a.rp_acct_id = b.rp_acct_id
	inner join building_use_generic_reclass c on a.use_desc = c.description
set a.residential_units = (a.proportion_residential * b.Revised_Tot_RP_Max_Apart_DU)
where c.generic_building_use_2 = 'R';

# Mobile Home Units
update sub_buildings a inner join pmc_table_2_mobile_home_du_by_rp_accnt_num b on a.rp_acct_id = b.rp_acct_id
	inner join building_use_generic_reclass c on a.use_desc = c.description
set a.residential_units = (a.proportion_residential * b.Mobile_home_records)
where c.generic_building_use_2 = 'R';

# Add additional units via Mark's email

# Census Tract = 080800
# 764/12 = 63.66 (only 12 "R" type buildings)
update sub_buildings a inner join building_use_generic_reclass b on a.use_desc = b.description
set a.residential_units = (a.residential_units + 63)
where 
	a.rp_acct_id = 1431808
	and b.generic_building_use_2 = 'R';

# Census Tract = 080800
# 1279/2 = 639 (only 2 "R" type buildings)
update sub_buildings a inner join building_use_generic_reclass b on a.use_desc = b.description
set a.residential_units = (a.residential_units + 1279)
where 
	a.rp_acct_id = 1346709
	and b.generic_building_use_2 = 'R';


# Delete records from sub_buildings table
#delete a.* from sub_buildings a where use_desc is null and flr_tot_sf = 0;

select sum(residential_units), count(*) from sub_buildings a where use_desc is null and flr_tot_sf = 0;
select sum(a.residential_units), count(*) from sub_buildings a left join parcels b on a.rp_acct_id = b.parcel_id where b.parcel_id is null;

/*-----------------------------
# Begin populating buildings table 
*/-----------------------------
drop table if exists buildings_bak;
create table buildings_bak select * from buildings;

drop table if exists buildings;
create table buildings
	(parcel_id varchar(100),
	building_id int(11),
	year_built int(11),
	gross_sqft int(11),
	net_sqft int(11),
	non_residential_sqft int(11),
	sqft_per_unit int(11),
	residential_units int(11),
	stories int(11),
	building_type_id int(11),
	building_quality_id int(11),
	improvement_value int(11),
	template_id int(11),
	job_capacity int(11),
	land_area int(11),
	tax_exempt int(11),
	number_of_buildings int(11),
	address varchar(100),
	stacked_flag int(11),
	present_use text,
	building_condition varchar(100),
	parcel_id_int int(11),
	stacked_parcel_id varchar(100),
	original_parcel_id varchar(100)
	);

alter table buildings add index parcel_id_index(parcel_id(10));
alter table buildings add index parcel_id_int_index(parcel_id_int);
alter table buildings add index present_use_index(present_use(10));

# Insert sub_building records into buildings table
insert into buildings
	(parcel_id,
	year_built,
	gross_sqft,
	stories,
	net_sqft,
	residential_units,
	present_use,
	building_condition,
	improvement_value)
select
	rp_acct_id as parcel_id,
	year_built as year_built,
	flr_tot_sf as gross_sqft,
	stories,
	flr_tot_sf as net_sqft,
	residential_units,
	use_desc as present_use,
	cond_cd as building_condition,
	improvement_value
from sub_buildings;

# Update non_residential_sqft
update buildings set non_residential_sqft = 0;

update buildings a inner join building_use_generic_reclass b on a.present_use = b.description 
set a.non_residential_sqft = a.net_sqft 
where b.generic_building_use_2 <> 'R';

/*
# Update sqft_per_unit
update buildings a inner join building_use_generic_reclass b on a.present_use = b.property_class
set a.sqft_per_unit = a.net_sqft 
where b.generic_building_use_2 = 'R';
*/

# Update tax exempt status
# NOTE - to be run once the parcels table has been created
update buildings a inner join parcels b on a.parcel_id = b.parcel_id
set a.tax_exempt = 1 where b.tax_exempt_flag = 1;


#---------------
# QC checkes
#---------------

# Check buildings not in parcels table
# NOTE - 6,639 records 5.23.2012
# NOTE - 6,736 records 5.24.2012 (did not remove building records where use_desc is null and flr_tot_sf = 0 bc units were imputed to these records
select count(*) from buildings a left join parcels b on a.parcel_id = b.parcel_id where b.parcel_id is null;

# Check parcels not in GIS table
# NOTE - 0 records 5.23.2012
select count(*) from parcels a left join kitfnl11 b on a.parcel_id = b.rp_acct_id where b.rp_acct_id is null;

# Check parcels w/o buildings 
# NOTE - 24,413 records 5.23.2012
# NOTE - 22,341 records 5.24.2012
select count(*) from parcels a left join buildings b on a.parcel_id = b.parcel_id where b.parcel_id is null;

#---------------
# Delete records
#---------------
# Create backup table of building before deleting records that are not in parcels table (after parcels table had delete records not in shapefile)
drop table if exists buildings_records_removed;
create table buildings_records_removed
select * from buildings;

# delete buildings records where records are not in parcels table
delete a.* from buildings a left join parcels b on a.parcel_id = b.parcel_id where b.parcel_id is null;
