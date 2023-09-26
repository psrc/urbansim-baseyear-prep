-- MHS 7/7/2020

-- create new parcel records where needed for PSNS, UW, and SeaTac airport
-- ALSO NEEDED:  Control total Change: UW Move 30,000 jobs from Block Group 1012 (530330053022) to Block Group 774 (530330053023), sector 13 Public Ed.

-- ==================================================================================================
/* BEFORE you start - make backup copies of Parcels and Buildings tables with today's date on them */
-- ==================================================================================================

USE 2018_parcel_baseyear_working_may2020;  -- modify if needed to where the tables are from the county data to region collating script

drop table if exists temp_bps_parcels;

create temporary table temp_bps_parcels
(
parcel_id int(11)
,parcel_id_fips varchar(30)
,county_id int(11)
,census_block_group_id int(11)
,zone_id	int(11)
,census_2010_block_id varchar(20)
,x_coord_sp double
,y_coord_sp double
,census_2010_block_group_id varchar(20)	
,census_block_id int(11)
,land_use_type_id int(11)	
,use_code int(11)	
,parking_space_daily int(11)
,parking_space_hourly int(11)
,parking_price_daily int(11)
,parking_price_hourly int(11)	
,census_tract_id int(11)
,grid_id int(11)
,parcel_sqft int(11)
);

-- SeaTac airport parcels fix - add 2 new parcels for Terminal and Cargo areas
-- PSNS add 5 new parcels - split base into 3 main sections plus the Southwest 'tail', plus one 'floating' polygon on the western edge.
-- UW explode original multi-polygon parcel into 6 discrete parcels, plus split the parking lot parcel at the stadium for the UW LINK parcel.

insert into temp_bps_parcels 
	(
	parcel_id
	,parcel_id_fips
	,county_id
	,census_block_group_id
	,zone_id
	,census_2010_block_id
	,x_coord_sp
	,y_coord_sp
	,census_2010_block_group_id
	,census_block_id
	,land_use_type_id
	,use_code
	,parking_space_daily
	,parking_space_hourly
	,parking_price_daily
	,parking_price_hourly
	,census_tract_id
	,grid_id
	,parcel_sqft
	)
values
(1302423, 'seatac1_term', 33, 254, 983, '530330284024052', 1277924.13541, 165190.968793, '530330284024', 55630, 22, 247, 0, 0, 0, 0, 0, 0, 1280417)
,(1302424, 'seatac2_cargo', 33, 254, 983, '530330284024007', 1277134.97269, 169152.420941, '530330284024', 55630, 22, 247, 0, 0, 0, 0, 0, 0, 165989)
,(1302425, 'psns1_main', 35, 1568, 3626, '530350814001007', 1196261.58496, 209569.873662, '530350814001', 30152, 7, 670, 0, 0, 0, 0, 0, 0, 9035426)
,(1302426, 'psns2_center', 35, 1568, 3626, '530350814001007', 1193574.61468, 208828.523174, '530350814001', 30152, 7, 670, 0, 0, 0, 0, 0, 0, 9704353)
,(1302427, 'psns3_west', 35, 1568, 3626, '530350814001031', 1191459.29633, 208393.078455, '530350814001', 31160, 7, 670, 0, 0, 0, 0, 0, 0, 7945088)
,(1302428, 'psns4_frag', 35, 1567, 3627, '530350811002019', 1190952.90476, 209368.837417, '530350811002', 59861, 7, 670, 0, 0, 0, 0, 0, 0, 95068)
,(1302429, 'psns5_tail', 35, 1566, 3629, '530350810003030', 1188683.11475, 205728.410211, '530350810003', 31277, 7, 670, 0, 0, 0, 0, 0, 0, 4627520)
,(1302430, 'uw1_misc', 33, 774, 303, '530330053023054', 1277600.4365, 240624.107392, '530330053023', 51391, 23, 184, 0, 0, 0, 0, 0, 0, 160882)
,(1302431, 'uw2_hosp', 33, 1012, 303, '530330053022019', 1276636.71632, 240522.394781, '530330053022', 52306, 23, 184, 0, 0, 0, 0, 0, 0, 1690681)
,(1302432, 'uw3_housing', 33, 772, 300, '530330041005021', 1281229.39463, 244199.256791, '530330041005', 27686, 23, 184, 0, 0, 0, 0, 0, 0, 304702)
,(1302433, 'uw4_parking', 33, 774, 301, '530330053023000', 1280171.61253, 243130.263496, '530330053023', 8295, 23, 184, 7499, 7499, 14, 3, 0, 0, 7462367)
,(1302434, 'uw5_maint', 33, 774, 301, '530330053023009', 1278952.55539, 244448.827534, '530330053023', 52319, 23, 184, 0, 0, 0, 0, 0, 0, 92091)
,(1302435, 'uw6_main', 33, 774, 302, '530330053023021', 1277273.07585, 242927.595009, '530330053023', 52331, 23, 184, 0, 0, 0, 0, 0, 0, 8556546)
,(1302436, 'uw7_stadium', 33, 774, 301, '530330053023006', 1278654.54988, 240895.563053, '530330053023', 52316, 23, 184, 0, 0, 0, 0, 0, 0, 2563362)
;

-- Update rest of census geographies via joins
update temp_bps_parcels a
	inner join 2018_parcel_baseyear_working_may2020.census_blocks b 
	on a.census_2010_block_id = b.census_2010_block_id 
set
	a.census_tract_id = b.census_tract_id;

-- import the new 'dummy' buildings records for psns and uw
-- PSNS add 3 new building records - different sizes to provide different weighting - 60% of jobs in east side, 20% central, 20% west
-- UW Main Campus add 59 new buildings to parcel_id 1302435, UW parcel 6
-- UW Stadium /Ath Dept add 1 building to parcel_id 1302436, UW parcel 7

drop table if exists temp_bps_buildings;

create temporary table temp_bps_buildings
	(
	parcel_id int(11)
	,building_id int(11)
	,parcel_id_fips varchar(20)
	,non_residential_sqft int(11)
	,gross_sqft int(11)
	,building_type_id int(11)
	,residential_units int(11)
	,county_id int(11)
	);

insert into temp_bps_buildings
	(
	parcel_id
	,building_id
	,parcel_id_fips
	,non_residential_sqft
	,gross_sqft
	,building_type_id
	,residential_units
	,county_id
	)
values
(1302425, 1218179, 'psns1_main', 600000, 600000, 5, 0, 35)
,(1302426, 1218180, 'psns2_center', 200000, 200000, 5, 0, 35)
,(1302427, 1218181, 'psns3_west', 200000, 200000, 5, 0, 35)
,(1302436, 1218182, 'uw7_stadium', 0, 0, 18, 0, 33)
,(1302435, 1218183, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218184, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218185, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218186, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218187, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218188, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218189, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218190, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218191, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218192, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218193, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218194, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218195, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218196, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218197, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218198, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218199, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218200, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218201, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218202, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218203, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218204, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218205, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218206, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218207, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218208, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218209, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218210, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218211, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218212, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218213, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218214, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218215, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218216, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218217, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218218, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218219, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218220, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218221, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218222, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218223, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218224, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218225, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218226, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218227, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218228, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218229, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218230, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218231, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218232, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218233, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218234, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218235, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218236, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218237, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218238, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218239, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218240, 'uw6_main', 0, 0, 18, 0, 33)
,(1302435, 1218241, 'uw6_main', 0, 0, 18, 0, 33)
;

-- delete original parcel records on UW and PSNS sites.  No buildings anyway.  
-- SeaTac airport original record just getting resized so it will continue to represent runways and open space

delete from parcels where parcel_id in (126703, 627565); 

update parcels
set parcel_sqft = 75080934
where parcel_id = 234060;  -- resizing SeaTac original parcel to account for subdivisions

-- ----------------------------------------------------------------------------
-- section with some specific building changes that aren't adding new buildings.
-- ----------------------------------------------------------------------------

-- SeaTac airport buildings fix - Move all buildings on orig airport parcel to new SeaTac Terminal parcel_id 1302423
update buildings
set parcel_id = 1302423
where parcel_id = 234060;

/* No longer needed per Hana - keeping statement until final testing
-- PSNS delete building 646700 - parcel right next to ferry terminal where jobs were all getting placed originally
delete from buildings where building_id = 646700; 
*/ 

-- UW Hosp change building_id 535731 from parcel_id 80321 to new parcel UW Hospital 1302431
update buildings
set parcel_id = 1302431
where building_id = 535731;

-- UW Hosp change building_id 523374 from parcel_id 80321 to 80312, moving to more central location in Block Group/campus area.
update buildings
set parcel_id = 80312
where building_id = 523374;

-- unrelated change, adjusting parcels just west of Tulalip commercial development to fall into the correct TAZ.  

update parcels
set 
x_coord_sp = x_coord_sp + 1000
,zone_id = 2172
where x_coord_sp between 1307475 and 1307525 and y_coord_sp between 399232 and 402426;

-- add records to the primary parcels table.

insert into parcels
	(
	parcel_id
	,parcel_id_fips
	,county_id
	,census_block_group_id
	,zone_id
	,census_2010_block_id
	,x_coord_sp
	,y_coord_sp
	,census_block_id
	,land_use_type_id
	,use_code
	,parking_space_daily
	,parking_space_hourly
	,parking_price_daily
	,parking_price_hourly
	,census_tract_id
	,grid_id
	)
select
	parcel_id
	,parcel_id_fips
	,county_id
	,census_block_group_id
	,zone_id
	,census_2010_block_id
	,x_coord_sp
	,y_coord_sp
	,census_block_id
	,land_use_type_id
	,use_code
	,parking_space_daily
	,parking_space_hourly
	,parking_price_daily
	,parking_price_hourly
	,census_tract_id
	,grid_id
from temp_bps_parcels;

-- add records to the primary buildings table.

insert into buildings
	(
	parcel_id
	,building_id
	,parcel_id_fips
	,non_residential_sqft
	,gross_sqft
	,building_type_id
	,residential_units
	,county_id
	)
select
	parcel_id
	,building_id
	,parcel_id_fips
	,non_residential_sqft
	,gross_sqft
	,building_type_id
	,residential_units
	,county_id
from temp_bps_buildings;

-- Clean up parcels table
update parcels set 	                      parcel_id	 = 0 where 	                      parcel_id	 is null;
update parcels set 	                 parcel_id_fips	 = 0 where 	                 parcel_id_fips	 is null;
update parcels set 	            parking_space_daily	 = 0 where 	            parking_space_daily	 is null;
update parcels set 	           parking_space_hourly	 = 0 where 	           parking_space_hourly	 is null;
update parcels set 	                     hschool_id	 = 0 where 	                     hschool_id	 is null;
update parcels set 	                        city_id	 = 0 where 	                        city_id	 is null;
update parcels set 	                    minority_id	 = 0 where 	                    minority_id	 is null;
update parcels set 	                census_tract_id	 = 0 where 	                census_tract_id	 is null;
update parcels set 	                        grid_id	 = 0 where 	                        grid_id	 is null;
update parcels set 	                         tod_id	 = 0 where 	                         tod_id	 is null;
update parcels set 	                census_block_id	 = 0 where 	                census_block_id	 is null;
update parcels set 	                     y_coord_sp	 = 0 where 	                     y_coord_sp	 is null;
update parcels set 	                      county_id	 = 0 where 	                      county_id	 is null;
update parcels set 	                   plan_type_id	 = 0 where 	                   plan_type_id	 is null;
update parcels set 	                       use_code	 = 0 where 	                       use_code	 is null;
update parcels set 	              transit_buffer_id	 = 0 where 	              transit_buffer_id	 is null;
update parcels set 	             school_district_id	 = 0 where 	             school_district_id	 is null;
update parcels set 	                     gross_sqft	 = 0 where 	                     gross_sqft	 is null;
update parcels set 	is_inside_urban_growth_boundary	 = 0 where 	is_inside_urban_growth_boundary	 is null;
update parcels set 	                 park_buffer_id	 = 0 where 	                 park_buffer_id	 is null;
update parcels set 	                     x_coord_sp	 = 0 where 	                     x_coord_sp	 is null;
update parcels set 	                        elem_id	 = 0 where 	                        elem_id	 is null;
update parcels set 	                  is_waterfront	 = 0 where 	                  is_waterfront	 is null;
update parcels set 	               growth_center_id	 = 0 where 	               growth_center_id	 is null;
update parcels set 	                         zip_id	 = 0 where 	                         zip_id	 is null;
update parcels set 	           parking_price_hourly	 = 0 where 	           parking_price_hourly	 is null;
update parcels set 	                         hex_id	 = 0 where 	                         hex_id	 is null;
update parcels set 	          regional_geography_id	 = 0 where 	          regional_geography_id	 is null;
update parcels set 	                  large_area_id	 = 0 where 	                  large_area_id	 is null;
update parcels set 	                     mschool_id	 = 0 where 	                     mschool_id	 is null;
update parcels set 	                     land_value	 = 0 where 	                     land_value	 is null;
update parcels set 	                   hct_block_id	 = 0 where 	                   hct_block_id	 is null;
update parcels set 	                 baseyear_built	 = 0 where 	                 baseyear_built	 is null;
update parcels set 	                     poverty_id	 = 0 where 	                     poverty_id	 is null;
update parcels set 	                        zone_id	 = 0 where 	                        zone_id	 is null;
update parcels set 	                   tractcity_id	 = 0 where 	                   tractcity_id	 is null;
update parcels set 	                         faz_id	 = 0 where 	                         faz_id	 is null;
update parcels set 	            growth_amenities_id	 = 0 where 	            growth_amenities_id	 is null;
update parcels set 	             is_in_transit_zone	 = 0 where 	             is_in_transit_zone	 is null;
update parcels set 	            parking_price_daily	 = 0 where 	            parking_price_daily	 is null;
update parcels set 	                     subarea_id	 = 0 where 	                     subarea_id	 is null;
update parcels set 	          census_block_group_id	 = 0 where 	          census_block_group_id	 is null;
update parcels set 	                   faz_group_id	 = 0 where 	                   faz_group_id	 is null;
update parcels set 	               land_use_type_id	 = 0 where 	               land_use_type_id	 is null;
update parcels set 	                   mix_split_id	 = 0 where 	                   mix_split_id	 is null;
update parcels set 	                    parcel_sqft	 = 0 where 	                    parcel_sqft	 is null;
update parcels set 	                  uga_buffer_id	 = 0 where 	                  uga_buffer_id	 is null;
update parcels set 	                      exemption	 = 0 where 	                      exemption	 is null;
update parcels set 	              stacked_parcel_id	 = 0 where 	              stacked_parcel_id	 is null;
update parcels set 	                   stacked_flag	 = 0 where 	                   stacked_flag	 is null;
update parcels set 	             original_parcel_id	 = 0 where 	             original_parcel_id	 is null;
update parcels set 	                        address	 = 0 where 	                        address	 is null;
update parcels set 	           census_2010_block_id	 = 0 where 	           census_2010_block_id  is null;

-- Clean up buildings table
update buildings set 	parcel_id	 = 0 where 	parcel_id	 is null;
update buildings set 	building_id	 = 0 where 	building_id	 is null;
update buildings set 	building_id_fips = 0 where 	building_id_fips is null;
update buildings set 	gross_sqft	 = 0 where 	gross_sqft	 is null;
update buildings set 	non_residential_sqft	 = 0 where 	non_residential_sqft	 is null;
update buildings set 	year_built	 = 0 where 	year_built	 is null;
update buildings set 	parcel_id_fips	 = 0 where 	parcel_id_fips	 is null;
update buildings set 	job_capacity	 = 0 where 	job_capacity	 is null;
update buildings set 	land_area	 = 0 where 	land_area	 is null;
update buildings set 	building_quality_id	 = 0 where 	building_quality_id	 is null;
update buildings set 	improvement_value	 = 0 where 	improvement_value	 is null;
update buildings set 	stories	 = 0 where 	stories	 is null;
update buildings set 	tax_exempt	 = 0 where 	tax_exempt	 is null;
update buildings set 	building_type_id	 = 0 where 	building_type_id	 is null;
update buildings set 	template_id	 = 0 where 	template_id	 is null;
update buildings set 	sqft_per_unit	 = 0 where 	sqft_per_unit	 is null;
update buildings set 	residential_units	 = 0 where 	residential_units	 is null;
update buildings set 	not_demolish	 = 0 where 	not_demolish	 is null;
update buildings set 	use_code	 = 0 where 	use_code	 is null;
update buildings set 	county_id	 = 0 where 	county_id	 is null;
	

	