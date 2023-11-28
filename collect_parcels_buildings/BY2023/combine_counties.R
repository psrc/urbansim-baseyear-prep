# Script for combining counties' parcel and buildings datasets together.
# It should be run after the four county scripts ran (process_*.R) which 
# created csv files with parcels and buildings for each parcel.
# This script creates datasets parcels and buildings and stores them 
# in the psrc_2023_parcel_baseyear DB, as well as csv files.
#
# Hana Sevcikova, last update 11/27/2023
#

library(data.table)

write.result <- TRUE # it will overwrite the existing tables parcels & buildings

if(write.result) source("mysql_connection.R")


all_parcels <- parcels <- NULL
all_buildings <- buildings <- NULL

for(county in c("king", "kitsap", "pierce", "snohomish")){
    pcl <- fread(paste0("urbansim_parcels_all_", county, ".csv"))
    pcl[, `:=`(parcel_id_fips = as.character(parcel_id_fips), 
               land_use_type_id = as.integer(land_use_type_id))]
    all_parcels <- rbind(all_parcels, pcl, fill = TRUE)
    pcl <- fread(paste0("urbansim_parcels_", county, ".csv"))
    pcl[, `:=`(parcel_id_fips = as.character(parcel_id_fips), 
               land_use_type_id = as.integer(land_use_type_id))]
    parcels <- rbind(parcels, pcl, fill = TRUE)
    
    bld <- fread(paste0("urbansim_buildings_all_", county, ".csv"))
    bld[, parcel_id_fips := as.character(parcel_id_fips)]
    all_buildings <- rbind(all_buildings, bld, fill = TRUE)
    bld <- fread(paste0("urbansim_buildings_", county, ".csv"))
    bld[, parcel_id_fips := as.character(parcel_id_fips)]
    buildings <- rbind(buildings, bld, fill = TRUE)
}

fwrite(all_parcels, file = "parcels_4counties_all.csv")
fwrite(all_buildings, file = "buildings_4counties_all.csv")

fwrite(parcels, file = "parcels_4counties.csv")
fwrite(buildings, file = "buildings_4counties.csv")


if(write.result){
    db <- "psrc_2023_parcel_baseyear"
    connection <- mysql.connection(db)
    dbWriteTable(connection, "parcels_all", all_parcels, overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "buildings_all", all_buildings, overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "parcels_prelim", parcels, overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "buildings_prelim", buildings, overwrite = TRUE, row.names = FALSE)
    DBI::dbDisconnect(connection)
}
