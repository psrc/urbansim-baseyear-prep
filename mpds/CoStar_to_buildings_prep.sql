use psrc_2023_parcel_baseyear_working;

alter table pipeline091924_prj_prcl23 add index parcel_id_index(parcel_id);
alter table pipeline091924_prj_prcl23 add index parcel_num_index(parcel_num(20));
alter table pipeline091924_prj_prcl23 add column residential_units int(11);

alter table pipeline091924_prj_prcl23 change column number_o_1 number_o_1 int(11);
alter table pipeline091924_prj_prcl23 change column number_o_2 number_o_2 int(11);
alter table pipeline091924_prj_prcl23 change column number_o_3 number_o_3 int(11);
alter table pipeline091924_prj_prcl23 change column number_o_4 number_o_4 int(11);

update pipeline091924_prj_prcl23 
set number_o_1 = 0 where number_o_1 = '';

update pipeline091924_prj_prcl23 
set number_o_2 = 0 where number_o_2 = '';

update pipeline091924_prj_prcl23 
set number_o_3 = 0 where number_o_3 = '';

update pipeline091924_prj_prcl23 
set number_o_4 = 0 where number_o_4 = '';

update pipeline091924_prj_prcl23 
set residential_units = 0;

update pipeline091924_prj_prcl23
set residential_units = (coalesce(number_o_2,0) + coalesce(number_o_3,0) + coalesce(number_o_4,0) + coalesce(number_o_1,0));

-- Fix records where parcel_id = 0 or parcel_id = 
update pipeline091924_prj_prcl23 set parcel_id = 703916 where propertyid = 12403781;
update pipeline091924_prj_prcl23 set parcel_id = 714042 where propertyid = 18892294;
update pipeline091924_prj_prcl23 set parcel_id = 728848 where propertyid = 15613161;
update pipeline091924_prj_prcl23 set parcel_id = 36551 where propertyid = 10288107;
update pipeline091924_prj_prcl23 set parcel_id = 44932 where propertyid = 11131791;
update pipeline091924_prj_prcl23 set parcel_id = 77322 where propertyid = 818181;
update pipeline091924_prj_prcl23 set parcel_id = 81036 where propertyid = 13898416;
update pipeline091924_prj_prcl23 set parcel_id = 495477 where propertyid = 19777843;
update pipeline091924_prj_prcl23 set parcel_id = 508071 where propertyid = 9694648;
update pipeline091924_prj_prcl23 set parcel_id = 508071 where propertyid = 9694688;
update pipeline091924_prj_prcl23 set parcel_id = 626784 where propertyid = 19676086;
update pipeline091924_prj_prcl23 set parcel_id = 828341 where propertyid = 13802148;
update pipeline091924_prj_prcl23 set parcel_id = 882648 where propertyid = 9777486;
update pipeline091924_prj_prcl23 set parcel_id = 944987 where propertyid = 11441445;
update pipeline091924_prj_prcl23 set parcel_id = 969277 where propertyid = 13886891;
update pipeline091924_prj_prcl23 set parcel_id = 1119337 where propertyid = 10780839;

update pipeline091924_prj_prcl23 a inner join psrc_2023_parcel_baseyear.parcels b on a.parcel_id = b.parcel_id set a.gis_sqft = b.gross_sqft where propertyid = 12403781;
update pipeline091924_prj_prcl23 a inner join psrc_2023_parcel_baseyear.parcels b on a.parcel_id = b.parcel_id set a.gis_sqft = b.gross_sqft where propertyid = 18892294;
update pipeline091924_prj_prcl23 a inner join psrc_2023_parcel_baseyear.parcels b on a.parcel_id = b.parcel_id set a.gis_sqft = b.gross_sqft where propertyid = 15613161;
update pipeline091924_prj_prcl23 a inner join psrc_2023_parcel_baseyear.parcels b on a.parcel_id = b.parcel_id set a.gis_sqft = b.gross_sqft where propertyid = 10288107;
update pipeline091924_prj_prcl23 a inner join psrc_2023_parcel_baseyear.parcels b on a.parcel_id = b.parcel_id set a.gis_sqft = b.gross_sqft where propertyid = 11131791;
update pipeline091924_prj_prcl23 a inner join psrc_2023_parcel_baseyear.parcels b on a.parcel_id = b.parcel_id set a.gis_sqft = b.gross_sqft where propertyid = 818181;
update pipeline091924_prj_prcl23 a inner join psrc_2023_parcel_baseyear.parcels b on a.parcel_id = b.parcel_id set a.gis_sqft = b.gross_sqft where propertyid = 13898416;
update pipeline091924_prj_prcl23 a inner join psrc_2023_parcel_baseyear.parcels b on a.parcel_id = b.parcel_id set a.gis_sqft = b.gross_sqft where propertyid = 19777843;
update pipeline091924_prj_prcl23 a inner join psrc_2023_parcel_baseyear.parcels b on a.parcel_id = b.parcel_id set a.gis_sqft = b.gross_sqft where propertyid = 9694648;
update pipeline091924_prj_prcl23 a inner join psrc_2023_parcel_baseyear.parcels b on a.parcel_id = b.parcel_id set a.gis_sqft = b.gross_sqft where propertyid = 9694688;
update pipeline091924_prj_prcl23 a inner join psrc_2023_parcel_baseyear.parcels b on a.parcel_id = b.parcel_id set a.gis_sqft = b.gross_sqft where propertyid = 19676086;
update pipeline091924_prj_prcl23 a inner join psrc_2023_parcel_baseyear.parcels b on a.parcel_id = b.parcel_id set a.gis_sqft = b.gross_sqft where propertyid = 13802148;
update pipeline091924_prj_prcl23 a inner join psrc_2023_parcel_baseyear.parcels b on a.parcel_id = b.parcel_id set a.gis_sqft = b.gross_sqft where propertyid = 9777486;
update pipeline091924_prj_prcl23 a inner join psrc_2023_parcel_baseyear.parcels b on a.parcel_id = b.parcel_id set a.gis_sqft = b.gross_sqft where propertyid = 11441445;
update pipeline091924_prj_prcl23 a inner join psrc_2023_parcel_baseyear.parcels b on a.parcel_id = b.parcel_id set a.gis_sqft = b.gross_sqft where propertyid = 13886891;
update pipeline091924_prj_prcl23 a inner join psrc_2023_parcel_baseyear.parcels b on a.parcel_id = b.parcel_id set a.gis_sqft = b.gross_sqft where propertyid = 10780839;

drop table if exists tmp_mpd_buildings;
create temporary table tmp_mpd_buildings
	(building_id int not null auto_increment,
	non_residential_sqft float,
	sqft_per_unit float,
	land_area float,
	improvement_value float,
	gross_sqft float,
	year_built int(11),
	parcel_id int(11),
	stories float,
	job_capacity int(11),
	building_type_id int(11),
	residential_units int(11),
	gis_sqft float,
primary key (building_id));

-- Insert Residential (Apartment buildings)
insert into tmp_mpd_buildings
	(non_residential_sqft,
	sqft_per_unit,
	land_area,
	improvement_value,
	gross_sqft,
	year_built,
	parcel_id,
	stories,
	job_capacity,
	building_type_id,
	residential_units,
	gis_sqft)
select
	0 as non_residential_sqft,
	0 as sqft_per_unit,
	land_area1 as land_area,
	0 as improvement_value,
	rba as gross_sqft,
	year_built as year_built,
	parcel_id as parcel_id,
	number_of_ as stories,
	0 as job_capacity,
	12 as building_type_id,
	residential_units as residential_units,
	gis_sqft as gis_sqft
from pipeline091924_prj_prcl23
where
	secondary_ in ('Apartments');
	
-- Mobile Home
insert into tmp_mpd_buildings
	(non_residential_sqft,
	sqft_per_unit,
	land_area,
	improvement_value,
	gross_sqft,
	year_built,
	parcel_id,
	stories,
	job_capacity,
	building_type_id,
	residential_units,
	gis_sqft)
select
	0 as non_residential_sqft,
	0 as sqft_per_unit,
	land_area1 as land_area,
	0 as improvement_value,
	rba as gross_sqft,
	year_built as year_built,
	parcel_id as parcel_id,
	number_of_ as stories,
	0 as job_capacity,
	11 as building_type_id,
	residential_units as residential_units,
	gis_sqft as gis_sqft
from pipeline091924_prj_prcl23
where	
	secondary_ in ('Manufactured Housing/Mobile Home Park');
	
-- Group Quarters
insert into tmp_mpd_buildings
	(non_residential_sqft,
	sqft_per_unit,
	land_area,
	improvement_value,
	gross_sqft,
	year_built,
	parcel_id,
	stories,
	job_capacity,
	building_type_id,
	residential_units,
	gis_sqft)
select
	0 as non_residential_sqft,
	0 as sqft_per_unit,
	land_area1 as land_area,
	0 as improvement_value,
	rba as gross_sqft,
	year_built as year_built,
	parcel_id as parcel_id,
	number_of_ as stories,
	0 as job_capacity,
	6 as building_type_id,
	residential_units as residential_units,
	gis_sqft as gis_sqft
from pipeline091924_prj_prcl23
where	
	secondary_ in ('Apartments (Student)', 'Assisted Living');	

-- Insert Commercial buildings	
insert into tmp_mpd_buildings
	(non_residential_sqft,
	sqft_per_unit,
	land_area,
	improvement_value,
	gross_sqft,
	year_built,
	parcel_id,
	stories,
	job_capacity,
	building_type_id,
	residential_units,
	gis_sqft)
select
	rba as non_residential_sqft,
	0 as sqft_per_unit,
	land_area1 as land_area,
	0 as improvement_value,
	rba as gross_sqft,
	year_built as year_built,
	parcel_id as parcel_id,
	number_of_ as stories,
	0 as job_capacity,
	3 as building_type_id,
	0 as residential_units,
	gis_sqft as gis_sqft
from pipeline091924_prj_prcl23
where
	secondary_ in ('Bank', 'Car Wash', 'Continuing Care Retirement Community', 'Convenience Store', 'Day Care Center', 'Department Store', 'Fast Food', 'Hotel', 'Restaurant', 'Retail Building', 'Storefront', 'Storefront Retail/Office');	

-- Schools
insert into tmp_mpd_buildings
	(non_residential_sqft,
	sqft_per_unit,
	land_area,
	improvement_value,
	gross_sqft,
	year_built,
	parcel_id,
	stories,
	job_capacity,
	building_type_id,
	residential_units,
	gis_sqft)
select
	rba as non_residential_sqft,
	0 as sqft_per_unit,
	land_area1 as land_area,
	0 as improvement_value,
	rba as gross_sqft,
	year_built as year_built,
	parcel_id as parcel_id,
	number_of_ as stories,
	0 as job_capacity,
	18 as building_type_id,
	0 as residential_units,
	gis_sqft as gis_sqft
from pipeline091924_prj_prcl23
where	
	secondary_ in ('Schools');

-- Other 
insert into tmp_mpd_buildings
	(non_residential_sqft,
	sqft_per_unit,
	land_area,
	improvement_value,
	gross_sqft,
	year_built,
	parcel_id,
	stories,
	job_capacity,
	building_type_id,
	residential_units,
	gis_sqft)
select
	rba as non_residential_sqft,
	0 as sqft_per_unit,
	land_area1 as land_area,
	0 as improvement_value,
	rba as gross_sqft,
	year_built as year_built,
	parcel_id as parcel_id,
	number_of_ as stories,
	0 as job_capacity,
	23 as building_type_id,
	0 as residential_units,
	gis_sqft as gis_sqft
from pipeline091924_prj_prcl23
where	
	secondary_ in ('Freestanding', 'Service');

-- Industrial
insert into tmp_mpd_buildings
	(non_residential_sqft,
	sqft_per_unit,
	land_area,
	improvement_value,
	gross_sqft,
	year_built,
	parcel_id,
	stories,
	job_capacity,
	building_type_id,
	residential_units,
	gis_sqft)
select
	rba as non_residential_sqft,
	0 as sqft_per_unit,
	land_area1 as land_area,
	0 as improvement_value,
	rba as gross_sqft,
	year_built as year_built,
	parcel_id as parcel_id,
	number_of_ as stories,
	0 as job_capacity,
	8 as building_type_id,
	0 as residential_units,
	gis_sqft as gis_sqft
from pipeline091924_prj_prcl23
where	
	secondary_ in ('Manufacturing');

-- Warehousing
insert into tmp_mpd_buildings
	(non_residential_sqft,
	sqft_per_unit,
	land_area,
	improvement_value,
	gross_sqft,
	year_built,
	parcel_id,
	stories,
	job_capacity,
	building_type_id,
	residential_units,
	gis_sqft)
select
	rba as non_residential_sqft,
	0 as sqft_per_unit,
	land_area1 as land_area,
	0 as improvement_value,
	rba as gross_sqft,
	year_built as year_built,
	parcel_id as parcel_id,
	number_of_ as stories,
	0 as job_capacity,
	21 as building_type_id,
	0 as residential_units,
	gis_sqft as gis_sqft
from pipeline091924_prj_prcl23
where		
	secondary_ in ('Distribution', 'Light Distribution', 'Self-Storage', 'Showroom', 'Warehouse');
	
-- Hospitals - Did not use building_type_id = 7 (hospitals) because there are not templates and UrbanSim does not build hospitals
insert into tmp_mpd_buildings
	(non_residential_sqft,
	sqft_per_unit,
	land_area,
	improvement_value,
	gross_sqft,
	year_built,
	parcel_id,
	stories,
	job_capacity,
	building_type_id,
	residential_units,
	gis_sqft)
select
	rba as non_residential_sqft,
	0 as sqft_per_unit,
	land_area1 as land_area,
	0 as improvement_value,
	rba as gross_sqft,
	year_built as year_built,
	parcel_id as parcel_id,
	number_of_ as stories,
	0 as job_capacity,
	3 as building_type_id,
	0 as residential_units,
	gis_sqft as gis_sqft
from pipeline091924_prj_prcl23
where		
	secondary_ in ('Hospital', 'Medical');
	
-- Mixed Use
insert into tmp_mpd_buildings
	(non_residential_sqft,
	sqft_per_unit,
	land_area,
	improvement_value,
	gross_sqft,
	year_built,
	parcel_id,
	stories,
	job_capacity,
	building_type_id,
	residential_units,
	gis_sqft)
select
	rba as non_residential_sqft,
	0 as sqft_per_unit,
	land_area1 as land_area,
	0 as improvement_value,
	rba as gross_sqft,
	year_built as year_built,
	parcel_id as parcel_id,
	number_of_ as stories,
	0 as job_capacity,
	10 as building_type_id,
	residential_units as residential_units,
	gis_sqft as gis_sqft
from pipeline091924_prj_prcl23
where		
	secondary_ in ('Office/Residential');

/*
-- 11-5-2024 Decided to not include parking records into MPD file
-- Parking
insert into tmp_mpd_buildings
	(non_residential_sqft,
	sqft_per_unit,
	land_area,
	improvement_value,
	gross_sqft,
	year_built,
	parcel_id,
	stories,
	job_capacity,
	building_type_id,
	residential_units,
	gis_sqft)
select
	0 as non_residential_sqft,
	0 as sqft_per_unit,
	land_area1 as land_area,
	0 as improvement_value,
	rba as gross_sqft,
	year_built as year_built,
	parcel_id as parcel_id,
	number_of_ as stories,
	0 as job_capacity,
	12 as building_type_id,
	residential_units as residential_units,
	gis_sqft as gis_sqft
from pipeline091924_prj_prcl23
where	
	secondary_ in ('Parking Garage', 'Parking Lot');
*/
	
-- TCU
insert into tmp_mpd_buildings
	(non_residential_sqft,
	sqft_per_unit,
	land_area,
	improvement_value,
	gross_sqft,
	year_built,
	parcel_id,
	stories,
	job_capacity,
	building_type_id,
	residential_units,
	gis_sqft)
select
	rba as non_residential_sqft,
	0 as sqft_per_unit,
	land_area1 as land_area,
	0 as improvement_value,
	rba as gross_sqft,
	year_built as year_built,
	parcel_id as parcel_id,
	number_of_ as stories,
	0 as job_capacity,
	20 as building_type_id,
	0 as residential_units,
	gis_sqft as gis_sqft
from pipeline091924_prj_prcl23
where	
	secondary_ in ('Utility Sub-Station');
	
-- Insert records with null secondary_ but has propertyty is not null (168 records)
-- General Commercial
insert into tmp_mpd_buildings
	(non_residential_sqft,
	sqft_per_unit,
	land_area,
	improvement_value,
	gross_sqft,
	year_built,
	parcel_id,
	stories,
	job_capacity,
	building_type_id,
	residential_units,
	gis_sqft)
select
	rba as non_residential_sqft,
	0 as sqft_per_unit,
	land_area1 as land_area,
	0 as improvement_value,
	rba as gross_sqft,
	year_built as year_built,
	parcel_id as parcel_id,
	number_of_ as stories,
	0 as job_capacity,
	3 as building_type_id,
	0 as residential_units,
	gis_sqft as gis_sqft
from pipeline091924_prj_prcl23
where	
	secondary_ = ''
	and propertyty in ('Office', 'Office (Lifestyle Center)', 'Office (Strip Center)', 'Retail', 'Retail (Community Center)', 'Retail (Neighborhood Center)', 'Retail (Power Center)', 'Retail (Strip Center)');

-- General Other
insert into tmp_mpd_buildings
	(non_residential_sqft,
	sqft_per_unit,
	land_area,
	improvement_value,
	gross_sqft,
	year_built,
	parcel_id,
	stories,
	job_capacity,
	building_type_id,
	residential_units,
	gis_sqft)
select
	rba as non_residential_sqft,
	0 as sqft_per_unit,
	land_area1 as land_area,
	0 as improvement_value,
	rba as gross_sqft,
	year_built as year_built,
	parcel_id as parcel_id,
	number_of_ as stories,
	0 as job_capacity,
	23 as building_type_id,
	0 as residential_units,
	gis_sqft as gis_sqft
from pipeline091924_prj_prcl23
where	
	secondary_ = ''
	and propertyty in ('Flex', 'Specialty');

-- General Other
insert into tmp_mpd_buildings
	(non_residential_sqft,
	sqft_per_unit,
	land_area,
	improvement_value,
	gross_sqft,
	year_built,
	parcel_id,
	stories,
	job_capacity,
	building_type_id,
	residential_units,
	gis_sqft)
select
	rba as non_residential_sqft,
	0 as sqft_per_unit,
	land_area1 as land_area,
	0 as improvement_value,
	rba as gross_sqft,
	year_built as year_built,
	parcel_id as parcel_id,
	number_of_ as stories,
	0 as job_capacity,
	8 as building_type_id,
	0 as residential_units,
	gis_sqft as gis_sqft
from pipeline091924_prj_prcl23
where	
	secondary_ = ''
	and propertyty in ('Industrial');	
	
-- 188 records where year_built = 0
update tmp_mpd_buildings 
set year_built = 2030 
where	
	year_built = 0;
	
drop table if exists buildings_mpd_in;
create table buildings_mpd_in 
select * from tmp_mpd_buildings;

/*
-- Summary of buildings before
select
	a.building_type_id,
	b.description,
	count(*) as cnt,
	sum(residential_units) as units,
	sum(non_residential_sqft) as nonres_sqft
from buildings_mpd_in a inner join psrc_2023_parcel_baseyear.building_types b on a.building_type_id = b.building_type_id
where	
	year_built <> 0
group by 
	a.building_type_id;


-- Summary of buildings after
select
	a.building_type_id,
	b.description,
	count(*) as cnt,
	sum(residential_units) as units,
	sum(non_residential_sqft) as nonres_sqft
from buildings_out a inner join psrc_2023_parcel_baseyear.building_types b on a.building_type_id = b.building_type_id
where
	template_id <> -1
	and year_built <> 0
group by 
	a.building_type_id;

-- Check against existing 2023 buildings
drop table if exists tmp_buildings_2023;
create temporary table tmp_buildings_2023
select
	parcel_id,
	count(*) as cnt,
	sum(residential_units) as units,
	sum(non_residential_sqft) as sqft
from psrc_2023_parcel_baseyear.buildings
group by 
	parcel_id;
	
alter table tmp_buildings_2023 add index parcel_id_index(parcel_id);

drop table if exists buildings_mpd_vs_existing;
create temporary table buildings_mpd_vs_existing
select 
	a.parcel_id,
	sum(a.residential_units) as mpd_units,
	sum(a.non_residential_sqft) as mpd_sqft,
	sum(b.units) as existing_units,
	sum(b.sqft) as existing_sqft
from buildings_out a 
	inner join tmp_buildings_2023 b on a.parcel_id = b.parcel_id
where 
	template_id <> -1
	and a.year_built <> 0
group by 
	a.parcel_id;

*/	
-- Create development project proposals table (512 records)
drop table if exists development_projet_proposals_new_2023;
create table development_projet_proposals_new_2023
select * from psrc_2023_parcel_baseyear.development_project_proposals;

-- Insert residential buildings
insert into development_projet_proposals_new_2023 
	(start_year,
	status_id,
	is_redevelopment,
	parcel_id,
	units_proposed,
	template_id)
select	
	year_built as start_year,
	3 as status_id,
	0 as is_redevelopment,
	parcel_id as parcel_id,
	residential_units as units_proposed,
	template_id as template_id
from psrc_2023_parcel_baseyear_working.buildings_out
where
	building_type_id in (12)
	and template_id <> -1;
	
-- Insert non-residential buildings
insert into development_projet_proposals_new_2023 
	(start_year,
	status_id,
	is_redevelopment,
	parcel_id,
	units_proposed,
	template_id)
select	
	year_built as start_year,
	3 as status_id,
	0 as is_redevelopment,
	parcel_id as parcel_id,
	non_residential_sqft as units_proposed,
	template_id as template_id
from psrc_2023_parcel_baseyear_working.buildings_out
where
	building_type_id not in (12)
	and template_id <> -1;
	
/* Additional QC of hospitals records
select 
	property_n, 
	property_a, 
	year_built, 
	secondary_, 
	sum(rba), 
	count(*) 
from pipeline091924_prj_prcl23 
where 
	year_built <> 0 
	and secondary_ in ('Hospital', 'Medical') 
group by 
	year_built, 
	secondary_, 
	property_a, 
	property_n;

select
	
	
*/