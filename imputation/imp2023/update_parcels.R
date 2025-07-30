# Script for updating parcel_sqft by taking into account footprints of buildings.
# In addition, it adds the column control_hct_id from
# control_id, growth_center_id and tod_id (parcels attributes)
# and control_rgs_id (controls attribute).
#
# Hana Sevcikova, 07/30/2025
#

library(data.table)

setwd("~/psrc/urbansim-baseyear-prep/imputation/imp2023")

save.into.mysql <- TRUE
save.as.csv <- TRUE

#pclout.name <- "parcel_sqft_land_area" # name of the output table/file
pclout.name <- "parcels_upd_sqft_hct"

# load buildings
bld.file.name <- "buildings_imputed_phase4_capacity_20241105.csv" # latest buildings table
data.year <- 2023 # data files will be taken from "../data{data.year}"
data.dir <- file.path("..", paste0("data", data.year))
bld <- fread(file.path(data.dir, bld.file.name))

# load parcels - only need the gross_sqft and parcel_sqft(_from_gis) columns, 
# as well as tod_id, control_id and growth_center_id
#pcl <- fread(file.path(data.dir, 'parcels_sqft.csv'))
pcl <- fread(file.path(data.dir, 'parcels.csv'))

# compute land area on each parcel
bldpcl <- bld[, .(land_area = sum(land_area)), by = "parcel_id"]

# join with parcels
pcl[bldpcl, land_area := i.land_area, on = "parcel_id"][, parcel_sqft := pmin(gross_sqft, parcel_sqft_from_gis)]

# compute parcel_sqft
pcl[parcel_sqft_from_gis < gross_sqft & !is.na(land_area), parcel_sqft := pmin(pmax(parcel_sqft_from_gis, land_area), gross_sqft)]
pcl[is.na(land_area) | is.infinite(land_area), land_area := 0]

# set control_hct_id from control_id, growth_center_id, control_rgs_id and tod_id
# load controls table and join with parcels
# (required columns are control_id and control_rgs_id)
controls <- fread(file.path(data.dir, 'controls.csv'))
pcl[controls, control_rgs_id := i.control_rgs_id, on = "control_id"]
# assign control_hct_id
pcl[, control_hct_id := control_id]
pcl[control_id < 200 & control_rgs_id < 4 & (tod_id > 0 | (growth_center_id >= 500 & growth_center_id <= 600)), 
                                             control_hct_id := control_id + 1000]

# remove unnecessary columns
pcl[, `:=`(land_area = NULL, control_rgs_id = NULL)]

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
