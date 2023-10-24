# Script to create parcels and buildings tables from Snohomish assessor data
# for the use in urbansim 
#
# Hana Sevcikova, last update 10/23/2023
#

library(data.table)

county <- "Snohomish"

data.dir <- file.path("~/e$/Assessor23", county) # path to the Assessor text files
misc.data.dir <- "data" # path to the BY2023/data folder
write.result.to.mysql <- TRUE # it will overwrite the existing tables urbansim_parcels & urbansim_buildings

if(write.result.to.mysql) source("mysql_connection.R")

###############
# Load all data
###############

# parcels & tax accounts
#parcels.23to18 <- fread(file.path(data.dir, "allparcels23_snoh_to_2018_parcels.txt"))
parcels.23to18 <- fread(file.path(data.dir, "parcels23_snoh_to_2018_parcels_short.txt"))

# reclass tables
lu_reclass <- fread(file.path(misc.data.dir, "land_use_generic_reclass_2023.csv"))
bt_reclass <- fread(file.path(misc.data.dir, "building_use_generic_reclass_2023.csv"))

# buildings
raw_buildings <- fread(file.path(data.dir, "improvement.txt"))

###################
# Process parcels
###################
# make column names lowercase
colnames(parcels.23to18) <- tolower(colnames(parcels.23to18))

# remove duplicates if any and records with parcel_id = NA
parcels.nodupl <- parcels.23to18[!is.na(parcel_id)][!duplicated(parcel_id)]
cat("\nNumber of records removed from parcels (either NA or duplicates): ", 
    nrow(parcels.23to18) - nrow(parcels.nodupl), "\n")

prep_parcels <- copy(parcels.nodupl)

# join with reclass table
prep_parcels[lu_reclass[county_id == 61], land_use_type_id := i.land_use_type_id, 
              on = c(usecode = "county_land_use_description")]

cat("\nMatched", nrow(prep_parcels[!is.na(land_use_type_id)]), "records with land use reclass table")
cat("\nUnmatched: ", nrow(prep_parcels[is.na(land_use_type_id)]), "records.")
cat("\nThe following codes were not found:\n")
print(prep_parcels[is.na(land_use_type_id) & !is.na(usecode), .N, by = "usecode"][order(usecode)])

###################
# Process buildings
###################
# make column names lowercase
colnames(raw_buildings) <- tolower(colnames(raw_buildings))

prep_buildings <- raw_buildings[, .(building_id = 1:nrow(raw_buildings),
                                    parcel_id = pin, imprtype, usecode, usedesc,
                                    bldgtype, stories, yrbuilt, finsize,
                                    numberrooms, numbedrms, propext)]

# join with building reclass table
prep_buildings[bt_reclass[county_id == 61], building_type_id := i.building_type_id, 
               on = c(usecode = "county_building_use_code")]
cat("\nMatched", nrow(prep_buildings[!is.na(building_type_id)]), "records with building reclass table")
cat("\nUnmatched: ", nrow(prep_buildings[is.na(building_type_id)]), "records.")
cat("\nThe following building codes were not found:\n")
print(prep_buildings[is.na(building_type_id), .N, by = "usecode"][order(-N)])
