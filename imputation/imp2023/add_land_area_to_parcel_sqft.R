library(data.table)

setwd("~/psrc/urbansim-baseyear-prep/imputation/imp2023")

save.into.mysql <- TRUE
save.as.csv <- TRUE

pclout.name <- "parcel_sqft_land_area" # name of the output table/file
  
# load buildings
bld.file.name <- "buildings_imputed_phase3_lodes_20240904.csv" # latest buildings table
data.year <- 2023 # data files will be taken from "../data{data.year}"
data.dir <- file.path("..", paste0("data", data.year))
bld <- fread(file.path(data.dir, bld.file.name))

# load parcels - only need the gross_sqft and parcel_sqft(_from_gis) columns
pcl <- fread(file.path(data.dir, 'parcels_sqft.csv'))

# compute land area on each parcel
bldpcl <- bld[, .(land_area = sum(land_area)), by = "parcel_id"]

# join with parcels
pcl[bldpcl, land_area := i.land_area, on = "parcel_id"][, parcel_sqft := pmin(gross_sqft, parcel_sqft_from_gis)]

pcl[parcel_sqft_from_gis < gross_sqft & !is.na(land_area), parcel_sqft := pmin(pmax(parcel_sqft_from_gis, land_area), gross_sqft)]
pcl[is.na(land_area) | is.infinite(land_area), land_area := 0]

if(save.into.mysql) {
  source("../../collect_parcels_buildings/BY2023/mysql_connection.R")
  db <- "psrc_2023_parcel_baseyear"
  connection <- mysql.connection(db)
  dbWriteTable(connection, pclout.name, pcl, overwrite = TRUE, row.names = FALSE)
  DBI::dbDisconnect(connection)
}
if(save.as.csv) {
  fwrite(pcl, file.path(data.dir, paste0(pclout.name, ".csv")))
}
