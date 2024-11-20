# Script for consolidating MPDs into records by parcel & building type.
# It also imputes missing residential_units for MF records.
# # Hana Sevcikova, 2024/11/19
#

library(data.table)

setwd("~/psrc/urbansim-baseyear-prep/mpds")

save.into.mysql <- TRUE
read.from.mysql <- TRUE

mpd.table.name.in <- "buildings_mpd_in"
mpd.table.name.out <- "buildings_mpd_cons_in"

# this directory needs parcels.csv and possibly the mpd buildings table
data.dir <- "data2023" # used if read.from.mysql is FALSE

if(save.into.mysql || read.from.mysql){
  source("../collect_parcels_buildings/BY2023/mysql_connection.R")
  db <- "psrc_2023_parcel_baseyear_working"
  connection <- mysql.connection(db)
}

if(read.from.mysql){
  mpds <- data.table(dbReadTable(connection, mpd.table.name.in))
  # Need to get parcel sqft info, but the parcels table is in another DB. 
  # So open that connection for this operation only.
  connection2 <- mysql.connection("psrc_2023_parcel_baseyear")
  pcl <- data.table(dbGetQuery(connection2, "SELECT parcel_id, parcel_sqft FROM parcels"))
  bld <- data.table(dbGetQuery(connection2, "SELECT building_id, parcel_id, residential_units, non_residential_sqft, building_type_id, year_built, land_area FROM buildings"))
  DBI::dbDisconnect(connection2)
  
} else { # read from csv
  mpds <- fread(file.path(data.dir, paste0(mpd.table.name.in, ".csv")))
  pcl <- fread(file.path(data.dir, "parcels.csv"))
  bld <- fread(file.path(data.dir, "buildings.csv"))
}

# there is a mixed-use record, which can be converted into MF (Mark checked that it makes sense for this parcel)
mpds[building_type_id == 10] # check that it is just one record or records for which this makes sense
mpds[building_type_id == 10, building_type_id := 12]

# consolidate by parcel and building type
mpds[pcl, `:=`(parcel_sqft = i.parcel_sqft), on = "parcel_id"]
# TODO: if an MPD falls on an environmental feature, leave the land area as it is
mpdsc <- mpds[parcel_id > 0, 
              .(non_residential_sqft = sum(non_residential_sqft), 
                land_area = pmin(pmax(pmin(sum(land_area), mean(parcel_sqft)*0.7), land_area), mean(parcel_sqft)*0.95),
                  improvement_value = sum(improvement_value), gross_sqft = sum(gross_sqft),
                  year_built = min(year_built), stories = sum(stories), residential_units = sum(residential_units),
                  parcel_sqft = mean(parcel_sqft), Nagg = .N),
              by = c("parcel_id", "building_type_id")]

# make sure the land area is not larger than the parcel footprint
#mpdsc[pcl, `:=`(land_area = pmin(land_area, parcel_sqft)), on = "parcel_id"]

# remove "other" components
mpdsc <- mpdsc[building_type_id != 23]

# check building type of MPDs with zero units
# mpdsc[residential_units == 0 & non_residential_sqft == 0, .N, by = "building_type_id"]

# impute residential units for MF buildings
mpdsc[, residential_units_orig := residential_units]
mpdsc[, `:=`(N = .N), by = "parcel_id"]

mpdsc[residential_units == 0 & non_residential_sqft == 0 & building_type_id == 12,
     residential_units := round(gross_sqft / 800)]
# check the largest values
#mpdsc[residential_units_orig == 0 & non_residential_sqft == 0 & building_type_id == 12][order(-residential_units)]

# try to identify MPDs that were already built and remove them
bldpcl <- bld[, .(existing_du = sum(residential_units), existing_nr_sqft = sum(non_residential_sqft), 
                  existing_year_built = max(year_built), existing_land_area = sum(land_area)), by = c("parcel_id", "building_type_id")]
mpdb <- merge(mpdsc, bldpcl, by = c("parcel_id", "building_type_id"), all.x = TRUE)
mpdb[is.na(existing_du), existing_du := 0][is.na(existing_nr_sqft), existing_nr_sqft := 0]

# all candidates for removal
remcand <- mpdb[(existing_du > 0 | existing_nr_sqft > 0)  & year_built < 2026] 
# residential selection
remcandres <- remcand[non_residential_sqft == 0 & existing_du > 0][, dif := (residential_units - existing_du)/existing_du * 100 ][dif < 100 & existing_year_built > 2017]
# non-residential selection
remcandnr <- remcand[residential_units == 0 & existing_nr_sqft > 0][, dif := (non_residential_sqft - existing_nr_sqft)/existing_nr_sqft * 100 ][existing_year_built > 2020 & dif < 50]

# remove the selected MPDs
mpdb2 <- mpdb[!parcel_id %in% c(remcandres$parcel_id, remcandnr$parcel_id) ]

# for proposals that are smaller than existing built, add the existing built to them
mpdb2[non_residential_sqft == 0 & existing_du > residential_units] # no residential records
mpdb2[residential_units == 0 & existing_nr_sqft > non_residential_sqft | (non_residential_sqft - existing_nr_sqft)/existing_nr_sqft * 100 < 20,
      `:=`(non_residential_sqft = existing_nr_sqft + non_residential_sqft, 
           land_area = pmax(pmin(land_area + existing_land_area, parcel_sqft*0.7), land_area), add = TRUE)]

# impute missing or wrong land_area
mpdb2[land_area < 100, `:=`(land_area_new = pmin(pmax(land_area, parcel_sqft*0.7), gross_sqft))]

# assemble final table
mpds.final <- mpdb2[, .(building_id = 1:nrow(mpdb2), parcel_id, building_type_id, non_residential_sqft,
                        residential_units, land_area = round(land_area), improvement_value, gross_sqft,
                        year_built, stories)]
mpds.final[, sqft_per_unit := 1][residential_units > 0, sqft_per_unit := gross_sqft/residential_units]
mpds.final <- mpds.final[residential_units > 0 | non_residential_sqft > 0]

# export buildings table
if(save.into.mysql) {
  dbWriteTable(connection, mpd.table.name.out, mpds.final, overwrite = TRUE, row.names = FALSE)
}

if(save.into.mysql || read.from.mysql) DBI::dbDisconnect(connection)
