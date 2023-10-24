/*
Date: 9.26.2019
Author: Peter Caballero
Purpose: Assemble 2018 Snohomish County Parcels and Buildings tables

2018 Raw Assessor Files: 
On the web:  Snohomish County FTP site:  https://snohomishcountywa.gov/3183/FTP-Data-Downloads

For the parcel data:
	In Access format, download assr_roll:  ftp://ftp.snoco.org/Assessor/Assessor_roll/MS_Access/
	-- contains all the tables listed below
	* exemptions
	* legaldescr
	* maindata
	* nameaddr
	* segmerge

For the buildings data:  
	Obtained actual raw downloaded files from Peter 
	In Excel format, ftp://ftp.snoco.org/Assessor/Property_Characteristics/
	-- contains all the tables listed below
	* SnohomishCo Master Records_2018AV.xlsx
	* SnohomishCo Land Records_2018AV.xlsx
	* SnohomishCo Improvement Records_2018AV.xlsx
	
Easiest way to get them into MySQL (for me) is to import the XLSX Buildings table files into Access and then go the ODBC route to MySQL
Open Excel files first and delete first row, column number
Some fields in the Master and Land tables would not import:
	Master: 
	- Legal1
	- Legal2
	- Legal3
	- OwnerName1
	- OwnerAddress1

	Land:
	- GrantorName1
	- GrantorName2
	- GrantorName3
Also when importing Improvements from Access to MySQL via ODBC dropped wall and floor covering fields due to errors in importing that I didn't take time to troubleshoot

Not sure where the other parcel data files came from:
	* snohco_improvement_records_2014av
	* tbl_074_coml_data_for_appending
	* snofnl15
	* snoptfnl15

Import raw asssessor files from access database to MySQL database: C:\2018_Assessor_Data\Snohomish\FTP\Property_Characteristics

snohomish_2018_parcel_baseyear

* exemptions
* legaldescr
* maindata
* nameaddr
* segmerge
* snohco_improvement_records_2014av
* tbl_074_coml_data_for_appending
* snofnl15
* snoptfnl15

*/

--------------------------
-- Create indicies on relevant tables
--------------------------
alter table snofnl15 add index parcel_id_index(parcel_id);
alter table snofnl15 add index lrsn_index(lrsn);
alter table snoptfnl15 add index parcel_id_index(parcel_id);
alter table snoptfnl15 add index lrsn_index(lrsn);
alter table maindata add index parcel_number_index(parcel_number);
alter table maindata add index propid_index(propid);
alter table maindata add index alt_parcel_nr_index(alt_parcel_nr);
alter table nameaddr add index parcel_number_index(parcel_number);
alter table nameaddr add index propid_index(propid);
alter table snohco_improvement_records_2014av add index lrsn_index(lrsn);
alter table snohco_improvement_records_2014av add index pin_index(pin);
alter table tbl_074_coml_data_for_appending add index pin_index(`Parcel Identification Number`);
alter table tbl_074_coml_data_for_appending add index propid_index(`Property ID Number`);
alter table segmerge add index parcel_number_index(parcel_number);
alter table allparcels_dissolve_id3 add index parcel_id_index (parcel_id);

-- ------------------------
-- Prep tables and join snofnl15 (polygon) and snoptfnl15 (point) together
-- Purpose is to ensure that the x,y values from the point file have been appended/joined to the polygon file for ease of use
-- QC/QA check - both tables should have same amount of record
-- ------------------------


drop table if exists prep_parcels_1;
create table prep_parcels_1
select
	parcel_id, -- MHS Original value
	parcel_i_1, -- MHS this is the miniumum value when record has been ID'd as a stacked polygon (join_count field anything but a 1 means a stacked polygon)
	lrsn, -- MHS Original value
	lrsn_1, -- MHS this is the miniumum value when record has been ID'd as a stacked polygon
	join_count, -- MHS added this as a way to ID stacked polygons I think
	situsline1,
	status,
	left(usecode,3) as usecode,
	tvr,
	mkimp,
	mklnd,
	cuimp,
	culnd,
	mkttl,
	xmptdescr,
	x_coord,
	y_coord,
	poly_area
from allparcels_dissolve_id3;
-- 318,774 rows
	
alter table prep_parcels_1 add index parcel_id_index(parcel_id);
alter table prep_parcels_1 add index lrsn_index(lrsn);
alter table prep_parcels_1 add index usecode_index(usecode(3));


-- ------------------------
-- Building assembly process
-- ------------------------

-- Create property_class/buiding use table
-- NOTE exported reclass table to access to insert generic buildings use 1 and 2 values (should only run this query 1 time)

-- MHS cleanup of just 11 records with blank usecode field but something in UF19, per documentation other source for usecode

update snohomish_improvement set usecode = uf19 where usecode is null and uf19 is not null;

drop table if exists building_use_generic_reclass;	
create table building_use_generic_reclass
select
	imprtype as improvement_type,
	usecode as use_code,
	usedesc as use_desc,
	-- building_type as building_type,
	count(*) as count
from snohomish_improvement
group by
	imprtype,
	usecode,
	usedesc;
	-- building_type;
-- 134 rows

-- MHS Maybe make 2 correspondence tables for Snohomish- 1 for all imprtype Dwelling and Commercial, and one for Other
-- MHS Big challenge is saving some 80 records that might represent schools.

alter table building_use_generic_reclass add index use_code_index(use_code(10));
alter table building_use_generic_reclass add column generic_building_use_1 text, add column building_type_id int(11), add column building_type_name varchar(30);
update building_use_generic_reclass set generic_building_use_1 = 'na', building_type_id = 0, building_type_name = 'na';

-- MHS for now, I'm using the 2018 table I developed in February

update building_use_generic_reclass r inner join 2018_parcel_baseyear_working.building_use_generic_reclass_2018_v2 v on r.use_code = v.county_building_use_code
set r.generic_building_use_1 = v.generic_building_use_1, r.building_type_id = v.building_type_id, r.building_type_name = v.building_type_name
where v.county_id = 61;

update building_use_generic_reclass set building_type_id = 22 where use_code is null;
update building_use_generic_reclass set generic_building_use_1 = 8 where use_code is null;


/* Archiving Peter's original code	
-- Make some revisions to generic building use 1 (particularly NULL generic_building_use_1 values)
-- Update based on use_codes

update building_use_generic_reclass a 
	inner join psrc_2011_parcel_baseyear.building_use_generic_reclass b on a.use_code = b.county_building_use_code
set a.generic_building_use_1 = b.generic_building_use_1
where 
	b.county = 61;

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
*/ -- MHS End of archiving original building use correspondence code

-- ****************************
-- ****************************
-- Start of the buildings table work
-- ****************************
-- ****************************

-- Create prep_buildings_1 table
drop table if exists prep_buildings_1_bak;
create table prep_buildings_1_bak select * from prep_buildings_1;
drop table if exists prep_buildings_1;

-- MHS fields going into this table don't match raw Snoh_Improvement field names.  Switching to alternative command below this one

/*
create table prep_buildings_1
select
	recid as building_id,
	lrsnum,
	pin,
	imprtype,
	usecode,
	usedesc,
	buildingtype,
	stories,
	yearbuilt,
	finishedsquarefeet,
	numberbedrooms,
	buildingrecordtypeandnumber
from snohomish_improvement;
*/

-- MHS Corrected version of create_table that refers to actual file names.

create table prep_buildings_1
select
	ID as building_id,
	LRSNum as lrsnum,
	PIN as pin,
	ImprType as imprtype,
	UseCode as usecode,
	UseDesc as usedesc,
	BldgType as buildingtype,
	Stories as stories,
	YrBuilt as yearbuilt,
	FinSize as finishedsquarefeet,
	NumberRooms as numberrooms,
	NumBedRms as numberbedrooms,
	PropExt as buildingrecordtypeandnumber
from snohomish_improvement;
-- 266542 records

alter table prep_buildings_1 add index pin_index(pin(10));
alter table prep_buildings_1 add index lrsn_index(lrsnum);
alter table prep_buildings_1 add index building_id_index(building_id);
alter table prep_buildings_1 add index usecode_index(usecode(15));

-- Calculate proportion improvement value for building records
alter table prep_buildings_1 add column proportion double;
alter table prep_buildings_1 add column improvement_value int(11);

update prep_buildings_1 set proportion = 0;
update prep_buildings_1 set improvement_value = 0;

drop table if exists tmp_bldgs_impvalue;

-- MHS must be taking this from pudresmeter table, mkimp not in Snohomish_Master table.
-- Never mind, found it, in the original download "maindata" table.
-- (Q for PMC) start of other section of handling improvement value
-- (PMC) A: The improvement value is from the "allparcels_dissolve_id3" table. 
-- MHS editing to group by PIN as well given LRSNs that are zero

create temporary table tmp_bldgs_impvalue
select
  parcel_id,
	lrsn,
	sum(ifnull(mkimp,0)) as improvement_value
from prep_parcels_1
group by
  parcel_id,
	lrsn;
-- 318,774 records

alter table tmp_bldgs_impvalue add index parcel_id_index(parcel_id);
alter table tmp_bldgs_impvalue add index lrsn_id_index(lrsn);

-- Create sum total building sqft by pin
drop table if exists tmp_bldgs_sqft;
create temporary table tmp_bldgs_sqft
select
  pin,
	lrsnum,
	count(*) as count,
	sum(finishedsquarefeet) as total_sqft
from prep_buildings_1
group by
  pin,
	lrsnum;
-- 257,957 records

alter table tmp_bldgs_sqft add index pin_index(pin);
alter table tmp_bldgs_sqft add index lrsn_index(lrsnum);

-- Calculate proportion
update prep_buildings_1 a
	inner join tmp_bldgs_sqft b on a.pin = b.pin and a.lrsnum = b.lrsnum
set a.proportion = a.finishedsquarefeet/b.total_sqft;

-- MHS added to convert null proportions to 1 if there is only 1 building record on the lrsn - impacts 5040 records

drop table if exists tmp_lrsnums_w_1_building_record;
create temporary table tmp_lrsnums_w_1_building_record
select pin as pin_1rec, lrsnum as lrsnum_1rec, count(*) as records from prep_buildings_1 group by pin, lrsnum;
delete from tmp_lrsnums_w_1_building_record where records>1;
-- 252,345 records

alter table tmp_lrsnums_w_1_building_record add index pin_index(pin_1rec);
alter table tmp_lrsnums_w_1_building_record add index lrsn_index(lrsnum_1rec);

update prep_buildings_1 a
	inner join tmp_lrsnums_w_1_building_record b on a.pin = b.pin_1rec and a.lrsnum = b.lrsnum_1rec
set a.proportion = 1;
-- 5,040 records

update prep_buildings_1 a
	inner join tmp_bldgs_impvalue b on a.pin = b.parcel_id and a.lrsnum = b.lrsn
set a.improvement_value = a.proportion * b.improvement_value;
-- 260,576 records

-- Added 12.2.2019 PUD metered units estimates received from Steve Toy at Snohomish County
-- Import J:\Projects\2018_base_year\Snohomish\GIS\Parcels_Assessor04072019_withPUD_residential_meter_counts.shp into geodatabase and rename as pudresmeter
-- Export pudresmeter to snohomish_2018_parcel_baseyear database

-- MHS Check and see if these exist already otherwise comment out
-- alter table pudresmeter add index parcel_id_index(parcel_id);
-- alter table pudresmeter add index lrsn_index(lrsn);

alter table prep_buildings_1 add column residential_units int(11);
-- 266,542 records

update prep_buildings_1
set residential_units = 0;

-- MHS In the update queries below had to convert all the numbers to strings (added single quotes around all of them) based on this error message
-- https://stackoverflow.com/questions/16068993/error-code-1292-truncated-incorrect-double-value-mysql

-- Set units for SF building types
update prep_buildings_1
set
	residential_units = 1
where
	usecode in ('1', '5', '11','12','13');
-- 224,999 records

-- Set units for duplex
update prep_buildings_1
set
	residential_units = 2
where
	usecode in ('2');
-- 3,679 records

-- Set units for triplex
update prep_buildings_1
set
	residential_units = 3
where
	usecode in ('3');
-- 288 records

-- MHS Type 5 is not described, but the data shows the SQFT and bedrooms and building types are SF - so add 5 to the Res Units = 1 list above and delete the next section.

-- Set units for 5
/*update prep_buildings_1
set
	residential_units = 5
where
	usecode in (5);
*/

-- MHS Code change: using NumberRooms for units for single-record Apartment buildings
drop table if exists tmp_aparts_nonzero_rooms;
create temporary table tmp_aparts_nonzero_rooms
select * from prep_buildings_1
where usecode in ('APART') and numberrooms>0;

drop table if exists tmp_single_record_aparts_nonzero_rooms;
create temporary table tmp_single_record_aparts_nonzero_rooms
select *
from tmp_aparts_nonzero_rooms a
inner join tmp_lrsnums_w_1_building_record b -- created earlier when adjusting proportions
on a.pin = b.pin_1rec and a.lrsnum = b.lrsnum_1rec;
-- 743 records

update prep_buildings_1 a
inner join tmp_single_record_aparts_nonzero_rooms b on a.building_id = b.building_id -- corrected to building id
set a.residential_units = b.numberrooms;

-- MHS Code change: Assert 1 unit per each Condo unit rather than processing via stacked parcels and buildings
update prep_buildings_1
set
  residential_units = 1
where
  residential_units = 0
and usecode in ('51','52','53','61','62');
-- 17,185 records

-- MHS Original command copying PUD meter counts over to MF, now deprecated
/*
update prep_buildings_1 a
	inner join pudresmeter b on a.lrsnum = b.lrsn
set
	a.residential_units = (a.proportion * b.pud_mtr_ct)
where
	-- a.usecode in (4, 70, 'APART');
	-- a.usecode in (4, 51, 70, 'APART');
	-- a.usecode not in (1, 2, 3, 5, 11, 12, 13);
	a.usecode in ('4', '44', '51', '52', '53', '6', '61', '62', '63', '70', 'APART');  -- This one for initial run?
*/

/* QC records
drop table if exists tmp1;

create temporary table tmp1
select
	situscity,
	count(*) as count,
	sum(residential_units) as units,
	sum(finishedsquarefeet) as sqft,
	sum(improvement_value) as imp
from prep_buildings_1 a inner join allparcels_dissolve_id3 b on a.lrsnum = b.lrsn
group by
	situscity;
*/


-- /*
-- Added 12.13.2019
-- QC prep_parcels_1 parcel_id = 00680100100100 contains same sized parcels (stacked) condo 85 units.
-- QC prep_parcels_1 lrsn = 1172564;

-- MHS Before heading into stacked parcels first ID the PIN / lrsnum we are interested in - IE MF w/o housing units yet.
-- MHS found we can reasonably distribute DU to most of the remaining records by limiting transfer to only lrsn nums where sqft and pud units are 300 to 3000 sqft per unit.

drop table if exists tmp_mf_blgs_w_no_units_yet;
create temporary table tmp_mf_blgs_w_no_units_yet
select * from prep_buildings_1
where residential_units = 0 and usecode in ('4', '44', '51', '52', '53', '6', '61', '62', '63', '70', 'APART');
alter table tmp_mf_blgs_w_no_units_yet add index pin_index(pin);
alter table tmp_mf_blgs_w_no_units_yet add index lrsnum_index(lrsnum);
alter table tmp_mf_blgs_w_no_units_yet add column resonly_proportion_sqft float;
update tmp_mf_blgs_w_no_units_yet set resonly_proportion_sqft = 0;

drop table if exists tmp_pin_lrsn_w_mf_blds_no_units;
create temporary table tmp_pin_lrsn_w_mf_blds_no_units
select pin,lrsnum, count(*) as records, sum(finishedsquarefeet) as sqft, sum(proportion) as proportion
from prep_buildings_1
where residential_units = 0 and usecode in ('4', '44', '51', '52', '53', '6', '61', '62', '63', '70', 'APART')
group by pin,lrsnum;
alter table tmp_pin_lrsn_w_mf_blds_no_units add index pin_index(pin);
alter table tmp_pin_lrsn_w_mf_blds_no_units add index lrsnum_index(lrsnum);

update tmp_mf_blgs_w_no_units_yet a inner join tmp_pin_lrsn_w_mf_blds_no_units b on a.pin = b.pin and a.lrsnum = b.lrsnum
set a.resonly_proportion_sqft = a.finishedsquarefeet/b.sqft;

drop table if exists tmp_pud_unique_pin_lrsn_w_sum_metercount;
create temporary table tmp_pud_unique_pin_lrsn_w_sum_metercount
select parcel_id, lrsn, count(*) as records, sum(pud_mtr_ct) as pud_mtr_ct_sum
from pudresmeter
group by parcel_id,lrsn;
alter table tmp_pud_unique_pin_lrsn_w_sum_metercount add index parcel_id_index(parcel_id);
alter table tmp_pud_unique_pin_lrsn_w_sum_metercount add index lrsn_index(lrsn);

drop table if exists tmp_qualified_pud_unique_pin_lrsn_w_sum_metercount;
create temporary table tmp_qualified_pud_unique_pin_lrsn_w_sum_metercount
select a.parcel_id, a.lrsn, a.records, a.pud_mtr_ct_sum
from tmp_pud_unique_pin_lrsn_w_sum_metercount a
inner join tmp_pin_lrsn_w_mf_blds_no_units b on a.parcel_id = b.pin and a.lrsn = b.lrsnum
where b.sqft/a.pud_mtr_ct_sum between 300 and 3000;
-- 1174 records

alter table tmp_qualified_pud_unique_pin_lrsn_w_sum_metercount add index parcel_id_index(parcel_id);
alter table tmp_qualified_pud_unique_pin_lrsn_w_sum_metercount add index lrsn_index(lrsn);

update tmp_mf_blgs_w_no_units_yet a inner join tmp_qualified_pud_unique_pin_lrsn_w_sum_metercount b on a.pin = b.parcel_id and a.lrsnum = b.lrsn
set a.residential_units = round(b.pud_mtr_ct_sum * a.resonly_proportion_sqft,0);
-- 1,953 records

update prep_buildings_1 a inner join tmp_mf_blgs_w_no_units_yet b on a.building_id = b.building_id -- remember building id when transfering back to prep_buildings table
set a.residential_units = b.residential_units
where a.residential_units = 0
  and a.usecode in ('4', '44', '51', '52', '53', '6', '61', '62', '63', '70', 'APART');
  
-- MHS After this new code, we are down to only 350 Res-type buildings left with no DU, including 220 apartment buildings.
-- MHS But all these records would have SQFT per unit less than 300 or greater than 3000 if we run with PUD meter counts

/*
-- MHS Diagnostic / QC tables
-- MHS Redo the list of buildings that still need units from earlier
drop table if exists tmp_mf_blgs_w_no_units_yet;
create temporary table tmp_mf_blgs_w_no_units_yet
select * from prep_buildings_1
where residential_units = 0 and usecode in ('4', '44', '51', '52', '53', '6', '61', '62', '63', '70', 'APART');
alter table tmp_mf_blgs_w_no_units_yet add index pin_index(pin);
alter table tmp_mf_blgs_w_no_units_yet add index lrsnum_index(lrsnum);

drop table if exists tmp_pin_lrsn_w_mf_blds_no_units;
create temporary table tmp_pin_lrsn_w_mf_blds_no_units
select pin,lrsnum, count(*) as records, sum(finishedsquarefeet) as sqft, sum(proportion) as proportion
from prep_buildings_1
where residential_units = 0 and usecode in ('4', '44', '51', '52', '53', '6', '61', '62', '63', '70', 'APART')
group by pin,lrsnum;
alter table tmp_pin_lrsn_w_mf_blds_no_units add index pin_index(pin);
alter table tmp_pin_lrsn_w_mf_blds_no_units add index lrsnum_index(lrsnum);

select a.pin, a.lrsnum, a.records, a.sqft, b.pud_mtr_ct_sum, round(a.sqft/b.pud_mtr_ct_sum,1) as sqft_per_unit
from tmp_pin_lrsn_w_mf_blds_no_units a inner join tmp_pud_unique_pin_lrsn_w_sum_metercount b
on a.pin = b.parcel_id and a.lrsnum = b.lrsn order by pud_mtr_ct_sum desc;
*/

-- MHS At this point PUD records are no help unless we go beyond reasonable SQFT per unit ratios.
-- Example problem: PIN 28053100203601, LRSN 4349786 is miscoded in Assessors data as APART - per website its actually the Heatherwood Apt Complex clubhouse.
-- But in PUD file it gets 260 units assigned to it, only 5,085 SQFT.
-- MHS Based on above findings, decided to impute units in remaining res buildings based on average so far by usecode

-- MHS Generate average SQFT per unit values up to this point by original Snohomish use codes for res buildings
drop table if exists tmp_res_ave_sqft_per_unit;
create temporary table tmp_res_ave_sqft_per_unit
select a.usecode, b.use_desc, sum(a.finishedsquarefeet) as sqft, sum(a.residential_units) as du, round(sum(a.finishedsquarefeet)/sum(a.residential_units),1) as sqft_per_unit from prep_buildings_1 a
inner join building_use_generic_reclass b on a.usecode = b.use_code
where
  usecode in ('1','2','3','5,','11','12','13','4', '44', '51', '52', '53', '6', '61', '62', '63', '70', 'APART')
  and residential_units>0
group by usecode;
alter table tmp_res_ave_sqft_per_unit add index usecode_index(usecode);

-- MHS Flag buildings where I imputed the sqft just in case
alter table prep_buildings_1 add column imputed_du_flag integer(5);
update prep_buildings_1 set imputed_du_flag = 0;

update prep_buildings_1
set imputed_du_flag = 1
where
  usecode in ('1','2','3','5,','11','12','13','4', '44', '51', '52', '53', '6', '61', '62', '63', '70', 'APART')
  and residential_units=0;

update prep_buildings_1 a inner join tmp_res_ave_sqft_per_unit b on a.usecode = b.usecode
set a.residential_units = round(a.finishedsquarefeet/b.sqft_per_unit,0)
where
  a.usecode in ('1','2','3','5,','11','12','13','4', '44', '51', '52', '53', '6', '61', '62', '63', '70', 'APART')
  and a.residential_units=0;
-- 327 rows

-- MHS Now going to trust PUD unit counts in non-residential buildings, after some research showed some split use buildings coded as commercial but QC versus Assessor website
-- definitely showed DU are present - like apartment complex where one building with attached units houses a bank office.

drop table if exists tmp_all_unique_pin_lrsnum_prep_bldgs_res_usecodes;
create temporary table tmp_all_unique_pin_lrsnum_prep_bldgs_res_usecodes
select
pin
,lrsnum
,count(*) as records
,sum(finishedsquarefeet) as sqft
,sum(residential_units) as du
from prep_buildings_1
where
usecode in ('1','2','3','5,','11','12','13','4', '44', '51', '52', '53', '6', '61', '62', '63', '70', 'APART')
group by
pin,lrsnum;
-- 244,766 records

alter table tmp_all_unique_pin_lrsnum_prep_bldgs_res_usecodes add index pin_index(pin);
alter table tmp_all_unique_pin_lrsnum_prep_bldgs_res_usecodes add index lrsnum_index(lrsnum);

drop table if exists tmp_all_unique_pin_lrsnum_prep_bldgs_other_usecodes;
create temporary table tmp_all_unique_pin_lrsnum_prep_bldgs_other_usecodes
select
pin
,lrsnum
,count(*) as records
,sum(finishedsquarefeet) as sqft
,sum(residential_units) as du
from prep_buildings_1
where
usecode not in ('1','2','3','5,','11','12','13','4', '44', '51', '52', '53', '6', '61', '62', '63', '70', 'APART')
group by
pin,lrsnum;
-- 8,677 records

alter table tmp_all_unique_pin_lrsnum_prep_bldgs_other_usecodes add index pin_index(pin);
alter table tmp_all_unique_pin_lrsnum_prep_bldgs_other_usecodes add index lrsnum_index(lrsnum);

drop table if exists all_unique_pin_lrsnum_prep_bldgs_all_usecodes;
create temporary table all_unique_pin_lrsnum_prep_bldgs_all_usecodes
select
pin
,lrsnum
,count(*) as records
from prep_buildings_1
group by
pin,lrsnum;
-- 257,957 records

alter table all_unique_pin_lrsnum_prep_bldgs_all_usecodes add index pin_index(pin);
alter table all_unique_pin_lrsnum_prep_bldgs_all_usecodes add index lrsnum_index(lrsnum);

alter table all_unique_pin_lrsnum_prep_bldgs_all_usecodes
add column res_records int
,add column res_sqft int
,add column res_du int
,add column other_records int
,add column other_sqft int
,add column other_du int;

update all_unique_pin_lrsnum_prep_bldgs_all_usecodes
set res_records = 0
,res_sqft = 0
,res_du = 0
,other_records = 0
,other_sqft = 0
,other_du = 0;

update all_unique_pin_lrsnum_prep_bldgs_all_usecodes a
inner join tmp_all_unique_pin_lrsnum_prep_bldgs_res_usecodes b
on a.pin = b.pin and a.lrsnum = b.lrsnum
set
a.res_records = b.records,
a.res_sqft = b.sqft,
a.res_du = b.du;

update all_unique_pin_lrsnum_prep_bldgs_all_usecodes a
inner join tmp_all_unique_pin_lrsnum_prep_bldgs_other_usecodes b
on a.pin = b.pin and a.lrsnum = b.lrsnum
set
a.other_records = b.records,
a.other_sqft = b.sqft,
a.other_du = b.du;

drop table if exists tmp_pud_units_by_pin_lrsn;
create temporary table tmp_pud_units_by_pin_lrsn
select
parcel_id
,lrsn
,count(*) as records
,sum(pud_mtr_ct) as pud_mtr_ct_sum
from pudresmeter
group by
parcel_id, lrsn;
-- 298,495 records

alter table tmp_pud_units_by_pin_lrsn add index parcel_id_index(parcel_id);
alter table tmp_pud_units_by_pin_lrsn add index lrsn_index(lrsn);

alter table all_unique_pin_lrsnum_prep_bldgs_all_usecodes add column pud_mtr_ct int;
update all_unique_pin_lrsnum_prep_bldgs_all_usecodes set pud_mtr_ct = 0;

update all_unique_pin_lrsnum_prep_bldgs_all_usecodes a
inner join tmp_pud_units_by_pin_lrsn b
on a.pin=b.parcel_id and a.lrsnum = b.lrsn
set a.pud_mtr_ct = b.pud_mtr_ct_sum;
-- 234,173 records

-- MHS criteria:  PIN-LRSN with no res buildings, no DU in current prep buildings table, non-zero PUD Res meter count, and SQFT per unit ratio between 300 and 3000

drop table if exists nonres_bldgs_to_add_pud_units_to;
create temporary table nonres_bldgs_to_add_pud_units_to
select a.building_id, a.pin, a.lrsnum, a.finishedsquarefeet, a.usecode, a.residential_units, b.pud_mtr_ct from prep_buildings_1 a
  inner join all_unique_pin_lrsnum_prep_bldgs_all_usecodes b
  on a.pin = b.pin and a.lrsnum = b.lrsnum
where b.other_du = 0 and b.res_records = 0 and b.pud_mtr_ct>0 and b.other_sqft>300 and b.other_sqft/b.pud_mtr_ct>300;
-- 707 records

alter table nonres_bldgs_to_add_pud_units_to add index building_id_index(building_id), add index pin_index(pin), add index lrsnum_index(lrsnum);
alter table nonres_bldgs_to_add_pud_units_to add column proportion_nonres_sqft float;
update nonres_bldgs_to_add_pud_units_to set finishedsquarefeet = 0 where finishedsquarefeet is null;

update nonres_bldgs_to_add_pud_units_to a inner join all_unique_pin_lrsnum_prep_bldgs_all_usecodes b on a.pin = b.pin and a.lrsnum = b.lrsnum
set a.proportion_nonres_sqft = a.finishedsquarefeet / b.other_sqft;

update nonres_bldgs_to_add_pud_units_to set residential_units = round(pud_mtr_ct*proportion_nonres_sqft,0);

-- select count(*) from nonres_bldgs_to_add_pud_units_to where residential_units = 0;

update prep_buildings_1 a inner join nonres_bldgs_to_add_pud_units_to b on a.building_id = b.building_id
set a.residential_units = b.residential_units where a.residential_units = 0;
-- 532 records

-- ****************************
-- ****************************
-- Start of the stacked parcels work
-- ****************************
-- ****************************

-- (Q for PMC):  The two dueling stacked parcels tables are generated here, which to use now? 
/*
(PMC) A: I think it's reasonable to use "our" stacked parcel data and fill in the residential unit information with the PUD data. I am not sure why I moved away from 
	"our" stacked parcel records and used the PUD file.
*/

-- Create stacked parcel records from prep_parcel_1 table
drop table if exists tmp_stacked_parcels_prep_parcel_1;
create temporary table tmp_stacked_parcels_prep_parcel_1
select
	poly_area,
	x_coord,
	y_coord,
	count(*) as cnt
from prep_parcels_1
group by
	poly_area,
	x_coord,
	y_coord
having cnt > 1;
-- 8,577 records

alter table tmp_stacked_parcels_prep_parcel_1 add index poly_area_index(poly_area);
alter table tmp_stacked_parcels_prep_parcel_1 add index x_coord_index(x_coord);
alter table tmp_stacked_parcels_prep_parcel_1 add index y_coord_index(y_coord);

-- MHS Renamed this, prior was 'mp_pud_stacked_with_pin', same temp table name used below for PUD, so we made this then blew it away without using before.
-- MHS by taking min parcel_id and LRSN you're arbitrarily picking one of the stacked records info to serve as the 'master' record

drop table if exists tmp_stacked_prep_parcels_1_with_pin;
create temporary table tmp_stacked_prep_parcels_1_with_pin
select
	min(b.parcel_id) as parcel_id,
	min(b.lrsn) as lrsn,
	a.poly_area,
	a.x_coord,
	a.y_coord,
	a.cnt
from tmp_stacked_parcels_prep_parcel_1 a
	inner join prep_parcels_1 b on a.poly_area = b.poly_area and a.x_coord = b.x_coord and a.y_coord = b.y_coord
group by
	a.poly_area,
	a.x_coord,
	a.y_coord,
	a.cnt;
alter table tmp_stacked_prep_parcels_1_with_pin add index parcel_id_index(parcel_id);
alter table tmp_stacked_prep_parcels_1_with_pin add index lrsn_index(lrsn);
alter table tmp_stacked_prep_parcels_1_with_pin add index poly_area_index(poly_area);
alter table tmp_stacked_prep_parcels_1_with_pin add index x_coord_index(x_coord);
alter table tmp_stacked_prep_parcels_1_with_pin add index y_coord_index(y_coord);



/* MHS Temporary hold QC & Diagnostics
select * from tmp_stacked_prep_parcels_1_with_pin;
select * from prep_parcels_1 where parcel_id in ('00576000800006');
select * from prep_parcels_1 where parcel_id in ('00568800200704');
select * from prep_parcels_1 where parcel_id in ('30052100400800');


select * from prep_parcels_1 where poly_area in ('4.14863638008') and x_coord in ('1305468.9208') and y_coord in ('357178.525269');
select * from allparcels_dissolve_id3 where poly_area in ('4.14863638008') and x_coord in ('1305468.9208') and y_coord in ('357178.525269');
select * from prep_buildings_1 where pin in ('00576000800006','00576000800007');

select * from allparcels_dissolve_id3 where lrsn = 0;
*/

/* MHS per Peter, this was used last time, but gonna archive it now and use the stacked info derived from the prep_parcels_1, allparcels_dissolve_id3, allparcels (IE orig Assessor data)

-- Create PUD stacked parcels
drop table if exists tmp_pud_stacked;
create temporary table tmp_pud_stacked
select
	shape_area,
	x_coord,
	y_coord,
	count(*) as cnt
from pudresmeter
group by
	shape_area,
	x_coord,
	y_coord
having cnt > 1;

alter table tmp_pud_stacked add index shape_area_index(shape_area);
alter table tmp_pud_stacked add index xcoord_index(x_coord);
alter table tmp_pud_stacked add index ycoord_index(y_coord);

-- select * from tmp_pud_stacked where shape_area like '419516%';

drop table if exists tmp_pud_stacked_with_pin;
create temporary table tmp_pud_stacked_with_pin
select
	min(b.parcel_id) as parcel_id,
	min(b.lrsn) as lrsn,
	a.shape_area,
	a.x_coord,
	a.y_coord,
	a.cnt
from tmp_pud_stacked a
	inner join pudresmeter b on a.shape_area = b.shape_area and a.x_coord = b.x_coord and a.y_coord = b.y_coord
group by
	a.shape_area,
	a.x_coord,
	a.y_coord,
	a.cnt;
alter table tmp_pud_stacked_with_pin add index parcel_id_index(parcel_id);
alter table tmp_pud_stacked_with_pin add index lrsn_index(lrsn);

-- MHS Temp hold QC & Diagnostics - compare contents of the two stacks
select * from tmp_stacked_prep_parcels_1_with_pin;
select * from tmp_pud_stacked_with_pin;

select * from tmp_stacked_prep_parcels_1_with_pin a
inner join tmp_pud_stacked_with_pin b on a.parcel_id = b.parcel_id and a.lrsn = b.lrsn;
where b.parcel_id is null and b.lrsn is null;


alter table tmp_pud_stacked_with_pin add column new_units int(11);
*/

/* MHS Archive additonal PUD stacked parcel work
alter table pudresmeter add index shape_area_index(shape_area), add index x_coord_index(x_coord), add index y_coord_index(y_coord);


drop table if exists tmp_pud_stacked_with_pin_units;
create table tmp_pud_stacked_with_pin_units
select
	a.parcel_id as min_parcel_id,
	a.lrsn as min_lrsn,
	a.cnt,
	a.new_units,
	b.*
from tmp_pud_stacked_with_pin a
	inner join pudresmeter b on a.shape_area = b.shape_area and a.x_coord = b.x_coord and a.y_coord = b.y_coord;

update tmp_pud_stacked_with_pin_units
set new_units = 1;

alter table tmp_pud_stacked_with_pin_units add index lrsn_index(lrsn);

-- MHS Here I think is where our sawmill property gets housing units.

/*  MHS Original code below where pud units where assigned to parcels via stacked parcel work - replaced with extra code in the buildings table section
-- MHS Temp Hold QC & Diagnostics tested the original stacked-parcels method of finishing out residential units
alter table prep_buildings_1 add column orig_residential_units int(11);
update prep_buildings_1 set orig_residential_units = 0;
update prep_buildings_1 set orig_residential_units = residential_units;

-- MHS modified below statements for test described above, changed res_units to orig_res_units so I could compare head to head in same table.
update prep_buildings_1 a
	inner join tmp_pud_stacked_with_pin_units b on a.lrsnum = b.lrsn
set
	a.orig_residential_units = b.new_units;

update prep_buildings_1 a
	inner join pudresmeter b on a.lrsnum = b.lrsn
set
	a.orig_residential_units = b.PUD_Mtr_Ct
where
	a.orig_residential_units = 0;
*/

-- ****************************
-- ****************************
-- Other stacked parcels results integrated back into buildings and parcels
-- ****************************
-- ****************************

-- Add columns and update prep_buildings_1 stacked parcels records
alter table prep_buildings_1 add column stacked_flag int(11);
alter table prep_buildings_1 add column stacked_parcel_id varchar(20);
alter table prep_buildings_1 add column original_parcel_id varchar(20);
alter table prep_buildings_1 add column stacked_lrsnum int(11);
alter table prep_buildings_1 add column original_lrsnum int(11);

-- MHS trying to match up this way instead of LRSN which is not unique
alter table prep_buildings_1 add column pin_poly_area double;
alter table prep_buildings_1 add column pin_x_coord double;
alter table prep_buildings_1 add column pin_y_coord double;


-- MHS Added initialization
update prep_buildings_1
set stacked_flag = 0,
	stacked_parcel_id = 'na',
	original_parcel_id = pin,
	stacked_lrsnum = 0,
	original_lrsnum = lrsnum,
	pin_poly_area = 0,
	pin_x_coord = 0,
	pin_y_coord = 0;

-- MHS Switching this to use the prep_parcels_1 version of stacked parcels - sidelining PUD now that we've mined all the unit information out of it we can

-- MHS corrections to match contents of tmp_stacked_prep_parcels_1_with_pin
-- MHS re-writing after finding LRSN is not a unique number
-- MHS Corrected version of query

update prep_buildings_1 a
	inner join prep_parcels_1 b on a.pin = b.parcel_id
set
  a.pin_poly_area = b.poly_area,
  a.pin_x_coord = b.x_coord,
  a.pin_y_coord = b.y_coord;
-- 266,314 records

-- alter table prep_parcels_1 add index poly_area_index(poly_area);
-- alter table prep_parcels_1 add index x_coord_index(x_coord);
-- alter table prep_parcels_1 add index y_coord_index(y_coord);

update prep_buildings_1 a
	inner join tmp_stacked_prep_parcels_1_with_pin b on a.pin_poly_area = b.poly_area and a.pin_x_coord = b.x_coord and a.pin_y_coord = b.y_coord
set
	a.stacked_flag = 1,
	a.stacked_parcel_id = b.parcel_id,
	a.original_parcel_id = a.pin,
  a.stacked_lrsnum = b.lrsn,
  a.original_lrsnum = a.lrsnum;
-- 40,085 records

/* MHS Original version
update prep_buildings_1 a
	inner join tmp_stacked_prep_parcels_1_with_pin b on a.lrsnum = b.lrsn
set
	a.stacked_flag = 1,
	a.stacked_parcel_id = b.lrsn,
	a.original_parcel_id = a.lrsnum
-- where
  -- b.min_lrns <> 0
;
*/

-- MHS This is where we switch the PIN and LRSN for stacked buildings to the nominal one chosen from the stack (using Minimum function)
-- MHS I think more LRSN numbers are impacted because of the zeros.

update prep_buildings_1 set lrsnum = stacked_lrsnum where stacked_flag = 1;
-- 37154 records

update prep_buildings_1 set pin = stacked_parcel_id where stacked_flag = 1;
-- 32,839 records

-- Add columns and update prep_parcels_1 stacked parcels records
alter table prep_parcels_1 add column stacked_flag int(11);
alter table prep_parcels_1 add column stacked_parcel_id varchar(20);
alter table prep_parcels_1 add column original_parcel_id varchar(20);
alter table prep_parcels_1 add column stacked_lrsn int(11);
alter table prep_parcels_1 add column original_lrsn int(11);

-- MHS instead doing this using the non-pud version
-- MHS switching to poly_area coord join, since LRSN is not unique in parcels table

update prep_parcels_1 a
	inner join tmp_stacked_prep_parcels_1_with_pin b on a.poly_area = b.poly_area and a.x_coord = b.x_coord and a.y_coord = b.y_coord
set
	a.stacked_flag = 1,
	a.stacked_parcel_id = b.parcel_id,
	a.original_parcel_id = a.parcel_id,
  a.stacked_lrsn = b.lrsn,
  a.original_lrsn = a.lrsn
#where
	#a.lrsn <> 0;
;
-- 53,285 records

/* MHS Original version
update prep_parcels_1 a
	inner join tmp_stacked_prep_parcels_1_with_pin b on a.lrsn = b.lrsn
set
	a.stacked_flag = 1,
	a.stacked_parcel_id = b.lrsn,
	a.original_parcel_id = a.lrsn
where
	a.lrsn <> 0;
*/

-- MHS only 3352 flagged
-- MHS Original PUD version commented out for now

/*
update prep_parcels_1 a
	inner join tmp_pud_stacked_with_pin_units b on a.lrsn = b.lrsn
set
	a.stacked_flag = 1,
	a.stacked_parcel_id = b.min_lrsn,
	a.original_parcel_id = a.lrsn
where
	a.lrsn <> 0;
*/
-- MHS make a backup of deleted parcels first just in case.
create table stacked_prep_parcels_1_to_be_deleted
select a.* from prep_parcels_1 a
where
	original_parcel_id <> stacked_parcel_id;

-- MHS confirmed all records deleted here have same poly_area and mklnd value as their 'parent' record
-- Remove stacked parcels from parcels table
delete a.* from prep_parcels_1 a
where
	original_parcel_id <> stacked_parcel_id;
-- 44,708 records deleted

-- QC  select * from buildings where parcel_id = 27043400201600\G
-- QC  select * from buildings where parcel_id = 00370300000001\G
-- QC  select * from buildings where parcel_id = 00370300000000\G

-- *********************
-- *********************
-- Stacked, overlapping assembly process
-- *********************
-- *********************

/*
GIS Steps: 
	1. convert \\FILE2\DataTeam\Projects\UrbanSim\NEW_DIRECTORY\GIS\Shapefiles\Parcels\Snohomish\2015\master.gdb\draft_snofnlpt15_nonbase
	2. spatial overlay \\FILE2\DataTeam\Projects\UrbanSim\NEW_DIRECTORY\GIS\Shapefiles\Parcels\Snohomish\2015\snofnlpt15_nonbase.shp and \\FILE2\DataTeam\Projects\UrbanSim\NEW_DIRECTORY\GIS\Shapefiles\Parcels\Snohomish\2015\draft1\snofnl15.shp
		output \\FILE2\DataTeam\Projects\UrbanSim\NEW_DIRECTORY\GIS\Shapefiles\Parcels\Snohomish\2015\snofnlpt15_nonbase_tobase.shp
	3. export to J:\Projects\UrbanSim\NEW_DIRECTORY\Databases\Access\Parcels\Snohomish\2015\downloads\May_12_2015\assr_roll.mdb
	4. export to snohomish_2014_parcel_baseyear
*/
-- ------------------------
-- (Q for PMC)  Where did snoflnpt15_nonbase_tobase come from - not in original MySQL table, and did not see it in the Access database
-- (PMC) A: The snoflpnt15 and snofnl15 are not being used in this process; part of this process was replaced by how the allparcels_dissolve_id3 file was created in GIS. 

/* Per Peter's comment above, commenting out this entire section
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
	inner join snofnl15 b on a.parcel_id = b.parcel_id;

alter table prep_stacked_buildings_already_exists_tobe_deleted_1 add index parcel_id_index(parcel_id);

delete a.*
from snoflnpt15_nonbase_tobase a
	inner join snofnl15 b on a.parcel_id = b.parcel_id;
*/-- End commented out section

-- ************************
-- ************************
-- Begin assembling the stacked parcels and associating it with the processed prep_buildings_1 table
-- ************************
-- ************************
--

-- MHS section not needed - improvement values already distributed earlier, buildings already tagged with their new stacked IDs
/*
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
	inner join snofnl15 b on a.base_parcel = b.parcel_id;
*/

-- *************************
-- *************************
-- Begin creating buildings table
-- *************************
-- *************************

-- MHS remember you already changed the pin and lrsnum to be the stacked versions where stacked_flag = 1 earlier
-- in the section, "Other stacked parcels results integrated back into buildings and parcels"

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
	parcel_id varchar(50),
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
	orig_parcel_id_fips varchar(50),
  stacked_parcel_id_fips varchar(50),
  lrsnum int(11),
  orig_lrsnum int(11),
  stacked_lrsnum int(11));


-- Insert data from res buildings into buildings table
insert into buildings
	(building_id,
	gross_sqft,
	sqft_per_unit,
	year_built,
	parcel_id,
	residential_units,
	improvement_value,
	use_code,
	stories,
  orig_parcel_id_fips,
  stacked_parcel_id_fips,
  lrsnum,
  orig_lrsnum,
  stacked_lrsnum)
select
	building_id as building_id,
	finishedsquarefeet as gross_sqft,
	round(finishedsquarefeet/residential_units,0) as sqft_per_unit,
	yearbuilt as year_built,
	pin as parcel_id,
	residential_units as residential_units,
	improvement_value as improvement_value,
	usecode as use_code,
	stories as stories,
  original_parcel_id as orig_parcel_id_fips,
  stacked_parcel_id as stacked_parcel_id_fips,
  lrsnum as lrsnum,
  original_lrsnum as orig_lrsnum,
  stacked_lrsnum as stacked_lrsnum
from prep_buildings_1
where
	usecode in ('APART', '1', '11', '12', '13', '2', '3', '4', '44', '5', '51', '52', '53', '6', '61', '62', '63', '70');


-- Insert non-residential buildings into buildings table
insert into buildings
	(building_id,
	gross_sqft,
  #non_residential_sqft,
	residential_units,
  year_built,
	parcel_id,
	improvement_value,
	use_code,
  stories,
  orig_parcel_id_fips,
  stacked_parcel_id_fips,
  lrsnum,
  orig_lrsnum,
  stacked_lrsnum)
select
	building_id,
	finishedsquarefeet as gross_sqft,
  residential_units as residential_units,
	#sqft as non_residential_sqft,
	yearbuilt as year_built,
	pin as parcel_id,
	improvement_value as improvement_value,
	usecode as use_code,
	stories as stories,
  original_parcel_id as orig_parcel_id_fips,
  stacked_parcel_id as stacked_parcel_id_fips,
  lrsnum as lrsnum,
  original_lrsnum as orig_lrsnum,
  stacked_lrsnum as stacked_lrsnum
from prep_buildings_1
where
	usecode not in ('APART', '1', '11', '12', '13', '2', '3', '4', '44', '5', '51', '52', '53', '6', '61', '62', '63', '70');

-- Insert buildings with null use codes into buildings table
insert into buildings
	(building_id,
	gross_sqft,
  #non_residential_sqft,
	residential_units,
  year_built,
	parcel_id,
	improvement_value,
	use_code,
  stories,
  orig_parcel_id_fips,
  stacked_parcel_id_fips,
  lrsnum,
  orig_lrsnum,
  stacked_lrsnum)
select
	building_id,
	finishedsquarefeet as gross_sqft,
  residential_units as residential_units,
	#sqft as non_residential_sqft,
	yearbuilt as year_built,
	pin as parcel_id,
	improvement_value as improvement_value,
	usecode as use_code,
	stories as stories,
  original_parcel_id as orig_parcel_id_fips,
  stacked_parcel_id as stacked_parcel_id_fips,
  lrsnum as lrsnum,
  original_lrsnum as orig_lrsnum,
  stacked_lrsnum as stacked_lrsnum
from prep_buildings_1
where
	usecode is null;

alter table buildings add index parcel_id_index(parcel_id(10));
alter table buildings add index building_id_index(building_id);
alter table buildings add index use_code_index(use_code(10));

/* MHS No longer needed
-- Update "stacked/overlapping" records' improvement_value in buildings table
update buildings a
	inner join prep_stacked_buildings_1 b on a.parcel_id = b.stacked_pin and a.building_id = b.building_id
set a.improvement_value = b.improvement_value;

-- Update "stacked/overlapping" records stacked_pin field
	-- it's not updating 5,888 records... why?
update buildings a
	inner join prep_stacked_buildings_1 b on a.parcel_id = b.stacked_pin and a.building_id = b.building_id
set a.stacked_pin = b.stacked_pin;
*/
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
/* -- MHS again think all this is now not needed
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
set orig_parcel_id_fips = 0
where orig_parcel_id_fips is null;

update buildings
set stacked_parcel_id_fips = 0
where stacked_parcel_id_fips is null;

update buildings
set lrsnum = 0
where lrsnum is null;

update buildings
set orig_lrsnum = 0
where orig_lrsnum is null;

update buildings
set stacked_lrsnum = 0
where stacked_lrsnum is null;

-- MHS Updated 5-20-20, moved this whole thing to after the Nulls are removed since they mess with the Greatest operator
-- Assign sqft per unit and nonres sqft to non-res buildings.  Chose 750 arbirtrarily after reviewing SQFT per unit results so far
-- MHS updated 5-19-20, was not returning the correct NonRes SQFT (including some negative numbers)

update buildings set sqft_per_unit = 750
where residential_units > 0 and
use_code not in ('APART', '1', '11', '12', '13', '2', '3', '4', '44', '5', '51', '52', '53', '6', '61', '62', '63', '70');

-- MHS Original Code
-- update buildings set non_residential_sqft = gross_sqft - greatest((sqft_per_unit*residential_units),0)
-- where use_code not in ('APART', '1', '11', '12', '13', '2', '3', '4', '44', '5', '51', '52', '53', '6', '61', '62', '63', '70');

-- MHS Revised code
update buildings set non_residential_sqft = greatest((gross_sqft - sqft_per_unit*residential_units),round(gross_sqft*0.20,0))
where use_code not in ('APART', '1', '11', '12', '13', '2', '3', '4', '44', '5', '51', '52', '53', '6', '61', '62', '63', '70');

-- MHS QC check is this thing working?
select
building_id
,gross_sqft
,non_residential_sqft
,sqft_per_unit
,residential_units
,gross_sqft - sqft_per_unit*residential_units as measure1
,round(gross_sqft*0.20,0) as measure2
,greatest(gross_sqft - sqft_per_unit*residential_units,round(gross_sqft*0.20,0)) as winner
from buildings where use_code = 'GENRET';

update buildings set sqft_per_unit = round((gross_sqft - non_residential_sqft)/residential_units,0)
where residential_units > 0 and use_code not in ('APART', '1', '11', '12', '13', '2', '3', '4', '44', '5', '51', '52', '53', '6', '61', '62', '63', '70');

-- ---------------------------
-- Applying Building Use Reclass from earlier - based on the v2 2018 table 
-- ---------------------------
-- MHS replaced original code with the building_use_generic_reclass we built earlier

update buildings a
	inner join building_use_generic_reclass b on a.use_code = b.use_code
set
	a.building_type_id = b.building_type_id;
	
-- MHS Moved parcels table work from beginning to end of script to pick up the final state of the prep_parcels_1  

-- ---------------------------
-- Create parcels table
-- ---------------------------
drop table if exists parcels_bak;
create table parcels_bak select * from parcels;

-- MHS Changes to carry forward both pin/parcel_id and lrsn, previously relied on lrsn as the parcel_id_fips, but there's 20,493 instances of LRSN = 0 in the allparcels_dissolve_id3 table

drop table if exists parcels;
create table parcels
	(parcel_id text,
	parcel_id_fips text,
	parcel_lrsn int(11),
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
	parcel_lrsn,
	land_value,
	improvement_value,
	use_code,
	exemption,
	parcel_sqft_in_gis,
	x_coord_sp,
	y_coord_sp,
	address)
select
	parcel_id as parcel_id_fips,
	lrsn as parcel_lrsn,
	mklnd as land_value,
	mkimp as improvement_value,
	usecode as use_code,
	xmptdescr as exemption,
	poly_area as parcel_sqft_in_gis,
	x_coord as x_coord_sp,
	y_coord as y_coord_sp,
	situsline1 as address 
from prep_parcels_1;
-- 318,774 rows

/* Diagnostics
select count(*) from parcels where improvement_value = 0;
select count(*) from parcels where land_value = 0;
select count(*) from parcels where parcel_sqft_in_gis = 0;
select count(*) from parcels where parcel_lrsn = 0;
select count(*) from parcels where parcel_id_fips =0;

-- MHS at this point 45,918 records have no improvement value
-- MHS at this point 14,120 records have no land value
-- MHS at this point all records have a parcel_sqft_in_gis value greater than 0
-- MHS at this point 20,493 records have a parcel_lrsn of 0
-- MHS at this point 1 record has a parcel_id_fips of 0
*/

alter table parcels add index parcel_id_index(parcel_id(10));
alter table parcels add index parcel_id_fips_index(parcel_id_fips(10));
alter table parcels add index parcel_lrsn_index(parcel_lrsn);
alter table parcels add index use_code_index(use_code(10));

-- Tax Exemption flag
update parcels set tax_exempt_flag = 0 where exemption is not null;

-- ------------------------
-- Parcel clean up
-- ------------------------
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
set address = 0 
where address is null;

-- MHS revised land use code assignment to merge prior 2014 and new list developed earlier.

/* MHS - Original code, replacement code below this.
update parcels a 
	inner join psrc_2014_parcel_baseyear_working.land_use_generic_reclass b on a.use_code = b.county_land_use_code 
set 
	a.land_use_type_id = b.land_use_type_id
where 
	b.county = 61;
*/

-- create land_use_generic_reclass table
drop table if exists land_use_generic_reclass;
create table land_use_generic_reclass
select
	left(usecode, 3) as use_code,
	usecode as use_desc,
	count(*) as count
from allparcels_dissolve_id3
group by
	usecode;
-- 255 rows

alter table land_use_generic_reclass add index use_code_index(use_code(3));

-- MHS Revised code
drop table if exists prior_land_use_generic_reclass;
create table prior_land_use_generic_reclass
select 
	county
	,county_land_use_code
	,land_use_description
	,generic_land_use_1
	,generic_land_use_type_id
	,land_use_type_id
from psrc_2014_parcel_baseyear_working.land_use_generic_reclass
where 
	county = 61
and county_land_use_code not in ('9999','mixed use 1 imputation','mixed use 2 imputation','0','NULL')
and county_land_use_code is not null;

-- 300 rows

alter table land_use_generic_reclass
	add column land_use_type_id int(11)
	,add column land_use_type_id_desc varchar(50)
	,add column generic_land_use_type_id int(11);
-- 255 rows

update land_use_generic_reclass
set 
	land_use_type_id = 0
	,land_use_type_id_desc = 'na'
	,generic_land_use_type_id = 0;
-- 255 rows

update land_use_generic_reclass n
inner join prior_land_use_generic_reclass o
on n.use_code = o.county_land_use_code
set
	n.land_use_type_id = o.land_use_type_id
	,n.land_use_type_id_desc = o.generic_land_use_1
	,n.generic_land_use_type_id = o.generic_land_use_type_id
where o.county = 61;
-- 251 rows

-- At this point you must review the current list for new codes and other translation errors first time thru the process.  
-- Below changes put in place for 2018 effort, including some corrections.  4-28-20

update land_use_generic_reclass set land_use_type_id = 15 where use_code in (145,187);
update land_use_generic_reclass set land_use_type_id = 14 where use_code in (121);
update land_use_generic_reclass set land_use_type_id = 10 where use_code in (239,309,390);
update land_use_generic_reclass set land_use_type_id = 3 where use_code in (187,564,590,624);
update land_use_generic_reclass set land_use_type_id = 27 where use_code in (912,916);
update land_use_generic_reclass set land_use_type_id = 1 where use_code in (860,821,830);
update land_use_generic_reclass set land_use_type_id = 25 where use_code in (475);
update land_use_generic_reclass set land_use_type_id = 17 where use_code in (' ');
update land_use_generic_reclass set land_use_type_id = 24 where use_code in (115);

drop table if exists land_use_types;
create table land_use_types select * from 2018_parcel_baseyear_working.land_use_types;

update land_use_generic_reclass g 
inner join land_use_types t on g.land_use_type_id = t.land_use_type_id
set 
	g.land_use_type_id_desc = t.description
	,g.generic_land_use_type_id = t.generic_land_use_type_id;
-- 65 rows

-- apply to parcels
update parcels a inner join land_use_generic_reclass b on a.use_code = b.use_code
set a.land_use_type_id = b.land_use_type_id; 
 
-- MHS Deleting a lot of records here.  20,493.  But what option do we have if no IDs to work with?  Maybe check XY for location?
-- MHS Updated 5-7-20.  Check this again, maybe not deleting so many now that we're using PIN instead of LRSN

drop table if exists parcels_no_parcel_id_fips;
create table parcels_no_parcel_id_fips
select a.* from parcels a
where parcel_id_fips = 0;
-- 1 row (after changing to using the PIN information instead of LRSN on 5-7-20
	
delete a.* from parcels a 
where parcel_id_fips = 0;

-- ------------------------
-- Delete records from buildings table that do not exists in parcels table
-- 2.9.2020 1745 records removed
-- ------------------------

drop table if exists buildings_before_delete_not_in_parcels;

create table buildings_before_delete_not_in_parcels
select *
from buildings;

delete a.*
from buildings a left join parcels b on a.parcel_id = b.parcel_id_fips
where b.parcel_id_fips is null;

-- 228 records, I can live with that.

-- ------------------------
-- Delete records from parcels table that do not exists in the parcel shapefile (aka snoptfnl15)
-- 2.9.2020 0 records
-- ------------------------
drop table if exists parcels_before_delete_not_in_gis;

create table parcels_before_delete_not_in_gis
select *
from parcels;

delete a.*
from parcels a left join allparcels_dissolve_id3 b on a.parcel_id_fips = b.parcel_id
where b.parcel_id is null;

-- no records deleted , as expected

-- -----------
-- MHS QC change, ID parcels coded as Mobile Home, change buildings on those parcels to mobile home.
-- -----------

drop table if exists temp_parcels_mobile_home_lutid;
create temporary table temp_parcels_mobile_home_lutid
select parcel_id_fips, land_use_type_id, use_code
from parcels
where land_use_type_id in (13);

update buildings a
inner join temp_parcels_mobile_home_lutid b on a.parcel_id = b.parcel_id_fips
set
	a.building_type_id = 11
where a.residential_units>0;

-- Add county id field to buildings table
alter table buildings add column county_id int(11);

update buildings
set county_id = 61;

alter table buildings add index county_id_index(county_id);


-- 11.5.2015 I need to QC records that were deleted...
