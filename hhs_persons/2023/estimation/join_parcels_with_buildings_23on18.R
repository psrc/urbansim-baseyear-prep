# Script for creating a parcels dataset that have attributes residential_units,
# number_of_buildings and x and y coordinates called "xcoord_p" and "ycoord_p".
# It's used for filtering parcels when assigning locations of agents in HH travel survey 
# (such as residence, work, school) to parcels.
# Hana Sevcikova, PSRC
# February 2023

library(data.table)
setwd("~/psrc/urbansim-baseyear-prep/hhs_persons/2023/estimation")

bld <- fread("../../../imputation/data2023/buildings_imputed_phase3_lodes_20240226.csv") # latest buildings table
pcl <- fread("../../../imputation/data2023/ref2018/parcels.csv") # parcels with attributes "parcel_id", "x_coord_sp", "y_coord_sp"

pbld <- bld[, .(residential_units = sum(residential_units), number_of_buildings = .N), by = .(parcel_id)]

setkey(pbld, "parcel_id")
setkey(pcl, "parcel_id")

pclm <- merge(pcl[, .(parcel_id, xcoord_p = x_coord_sp, ycoord_p = y_coord_sp)], pbld, all = TRUE)
pclm[is.na(residential_units), residential_units := 0]
pclm[is.na(number_of_buildings), number_of_buildings := 0]

fwrite(pclm, "parcels_for_hh_survey.csv")
