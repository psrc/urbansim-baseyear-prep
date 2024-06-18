# Script for combining counties' parcel and buildings datasets together.
# It should be run after the four county scripts ran (process_*.R) which 
# created csv files with parcels and buildings for each parcel.
# This script creates datasets parcels_prelim and buildings_prelim and stores them 
# in the psrc_2023_parcel_baseyear DB, as well as csv files.
#
# Hana Sevcikova, last update 06/17/2024
#

library(data.table)

write.result <- FALSE # it will overwrite the existing tables parcels & buildings

if(write.result) source("mysql_connection.R")


parcels <- buildings <- NULL

for(county in c("king", "kitsap", "pierce", "snohomish")){
    pcl <- fread(paste0("urbansim_parcels_", county, ".csv"), 
                 colClasses = c(parcel_id_fips = "character"))
    pcl[, `:=`(land_use_type_id = as.integer(land_use_type_id), gross_sqft = as.numeric(gross_sqft))]
    
    bld <- fread(paste0("urbansim_buildings_", county, ".csv"), 
                 colClasses = c(parcel_id_fips = "character"))[
                     , `:=`(county_id = pcl[1, county_id], gross_sqft = as.numeric(gross_sqft))]
    if("use_desc" %in% colnames(bld)) bld[, use_desc := NULL]
    
    parcels <- rbind(parcels, pcl, fill = TRUE)
    buildings <- rbind(buildings, bld, fill = TRUE)
}

# assign unique identifiers
parcels[, parcel_id := 1:nrow(parcels)]
buildings[, building_id := 1:nrow(buildings)]
buildings[parcels, parcel_id := i.parcel_id, on = .(parcel_id_fips, county_id)] 

fwrite(parcels, file = "parcels_4counties.csv")
fwrite(buildings, file = "buildings_4counties.csv")


if(write.result){
    db <- "psrc_2023_parcel_baseyear"
    connection <- mysql.connection(db)
    dbWriteTable(connection, "parcels_prelim", parcels, overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "buildings_prelim", buildings, overwrite = TRUE, row.names = FALSE)
    DBI::dbDisconnect(connection)
}


