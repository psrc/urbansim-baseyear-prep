# Script for consolidating MPDs into records by parcel & building type.
# It also imputes missing residential_units for MF records.
# # Hana Sevcikova, 2024/11/19
#

library(data.table)

setwd("~/psrc/urbansim-baseyear-prep/mpds")

save.into.mysql <- TRUE
read.from.mysql <- FALSE

mpd.table.name.in <- "buildings_mpd_in"
mpd.table.name.out <- "buildings_mpd_cons_in"
buildings.name.out <- "buildings_with_not_demolish" # output table of the buildings that will have the column not_demolish attached

# this directory needs parcels.csv and possibly the mpd buildings table
data.dir <- "data2023" # used if read.from.mysql is FALSE

if(save.into.mysql || read.from.mysql){
  source("../collect_parcels_buildings/BY2023/mysql_connection.R")
  db <- "psrc_2023_parcel_baseyear_working" # where to take the MPD table from
  db.main <- "psrc_2023_parcel_baseyear" # where to take parcels and buildings tables from
  connection <- mysql.connection(db)
  connection.main <- mysql.connection(db.main)
}

if(read.from.mysql){
  mpds <- data.table(dbReadTable(connection, mpd.table.name.in))
  pcl <- data.table(dbGetQuery(connection.main, "SELECT parcel_id, parcel_sqft FROM parcels"))
  bld <- data.table(dbGetQuery(connection.main, "SELECT * FROM buildings"))
} else { # read from csv
  mpds <- fread(file.path(data.dir, paste0(mpd.table.name.in, ".csv")))
  pcl <- fread(file.path(data.dir, "parcels.csv"))
  bld <- fread(file.path(data.dir, "buildings.csv"))
}

# there is a mixed-use record, which can be converted into MF (Mark checked that it makes sense for this parcel)
mpds[building_type_id == 10] # check that it is just one record or records for which this makes sense
mpds[building_type_id == 10, building_type_id := 12]

# consolidate by parcel and building type
mpds[pcl, `:=`(parcel_sqft = i.parcel_sqft, parcel_gross_sqft = i.gross_sqft), on = "parcel_id"]

# Derive buildings' footprint
# (the land_area attribute is taken from CoStar and it's not clear if it reflects the building footprint)
mpds[, costar_land_area := land_area][, land_area := NULL]
mpds[stories > 0, land_area := gross_sqft/stories] # it looks like yes, the costar land_area is exactly this ratio
mpds[stories == 0, far := gross_sqft/parcel_gross_sqft][, stories_new := pmax(1, cut(far, c(0, seq(0.5, 99, by = 1)), 
                                                                             include.lowest = TRUE, labels = FALSE) - 1)]
mpds[stories == 0, `:=`(land_area = gross_sqft/stories_new, stories = stories_new)] 
# set the building footprint not to be larger than 90% of the parcel size
mpds[land_area > parcel_gross_sqft, land_area := pmin(land_area, parcel_gross_sqft*0.9)]
mpds[, `:=`(far = NULL, stories_new = NULL)]

mpdsc <- mpds[parcel_id > 0, 
              .(non_residential_sqft = sum(non_residential_sqft), 
                land_area = pmin(sum(land_area), mean(parcel_gross_sqft)*0.9), # land area should not be larger than 90% of the parcel size
                  improvement_value = sum(improvement_value), gross_sqft = sum(gross_sqft),
                  year_built = min(year_built), stories = sum(stories), residential_units = sum(residential_units),
                  parcel_gross_sqft = mean(parcel_gross_sqft), Nagg = .N),
              by = c("parcel_id", "building_type_id")]


# remove "other" and MH components
mpdsc <- mpdsc[!building_type_id %in% c(23, 11)]

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
# analyze by summing for each parcel
bldpcl <- bld[building_type_id != 23, .(existing_du = sum(residential_units), existing_nr_sqft = sum(non_residential_sqft), 
                  existing_year_built = max(year_built), existing_land_area = sum(land_area)), by = "parcel_id"]
proposed <- mpdsc[, .(proposed_du = sum(residential_units), proposed_nr_sqft = sum(non_residential_sqft), 
                  proposed_year_built = min(year_built), proposed_land_area = sum(land_area)), by = "parcel_id"]

propbuilt <- merge(proposed, bldpcl, by = "parcel_id")

# all candidates for removal
remcand <- propbuilt[proposed_year_built < 2026 & existing_year_built > 2017] 
remcand[, `:=`(difres = (proposed_du - existing_du)/existing_du * 100, 
               difnonres =  (proposed_nr_sqft - existing_nr_sqft)/existing_nr_sqft * 100) ]
rem <- remcand[(proposed_du == 0 & difnonres < 50) | (proposed_nr_sqft == 0 & difres < 100)] # 20 proposals

# remove the selected MPDs
mpdb <- mpdsc[!parcel_id %in% rem$parcel_id]

# identify proposals on parcels that should not be demolished
propbuilt <- propbuilt[!parcel_id %in% rem$parcel_id]
not.demolish <- c(propbuilt[proposed_nr_sqft == 0 & proposed_du < existing_du, parcel_id],
                  propbuilt[proposed_du == 0 & proposed_nr_sqft < existing_nr_sqft, parcel_id],
                  propbuilt[proposed_nr_sqft > 0 & proposed_du == 0 & existing_du > 0 & existing_year_built > 2014, parcel_id],
                  propbuilt[proposed_du > 0 & proposed_nr_sqft == 0 & existing_nr_sqft > 0 & existing_year_built > 2014, parcel_id]
                  )
updbld <- bld[, not_demolish := as.integer(parcel_id %in% not.demolish)]

# assemble final MPD table
mpds.final <- mpdb[, .(building_id = 1:nrow(mpdb), parcel_id, building_type_id, non_residential_sqft,
                        residential_units, land_area = round(land_area), improvement_value, gross_sqft,
                        year_built, stories)]
mpds.final[, sqft_per_unit := 1][residential_units > 0, sqft_per_unit := gross_sqft/residential_units]
mpds.final <- mpds.final[residential_units > 0 | non_residential_sqft > 0] # this will remove 4 GQ records

# export MPD and the buildings table
if(save.into.mysql) {
  dbWriteTable(connection, mpd.table.name.out, mpds.final, overwrite = TRUE, row.names = FALSE)
  dbWriteTable(connection.main, buildings.name.out, updbld, overwrite = TRUE, row.names = FALSE)
}

if(save.into.mysql || read.from.mysql) {
  DBI::dbDisconnect(connection)
  DBI::dbDisconnect(connection.main)
}
