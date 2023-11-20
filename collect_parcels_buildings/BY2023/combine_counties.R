# Script for combining counties' parcel and buildings datasets together.
# It should be run after the four county scripts ran (process_*.R) which 
# created csv files with parcels and buildings for each parcel.
# This script creates datasets parcels and buildings and stores them 
# in the psrc_2023_parcel_baseyear DB, as well as csv files.
#
# Hana Sevcikova, last update 11/20/2023
#

library(data.table)

write.result <- FALSE # it will overwrite the existing tables parcels & buildings

if(write.result) source("mysql_connection.R")


all_parcels <- NULL
all_buildings <- NULL

for(county in c("king", "kitsap", "pierce", "snohomish")){
    pcl <- fread(paste0("urbansim_parcels_all_", county, ".csv"))
    pcl[, parcel_id_fips := as.character(parcel_id_fips)]
    all_parcels <- rbind(all_parcels, pcl, fill = TRUE)
    bld <- fread(paste0("urbansim_buildings_all_", county, ".csv"))
    bld[, parcel_id_fips := as.character(parcel_id_fips)]
    all_buildings <- rbind(all_buildings, bld, fill = TRUE)
}

fwrite(all_parcels, file = "parcels_all_counties.csv")
fwrite(all_buildings, file = "buildings_all_counties.csv")

if(write.result){
    db <- "psrc_2023_parcel_baseyear"
    connection <- mysql.connection(db)
    dbWriteTable(connection, "parcels", all_parcels, overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "buildings", all_buildings, overwrite = TRUE, row.names = FALSE)
    DBI::dbDisconnect(connection)
}
