# Script for creating a parcels dataset that have attributes residential_units,
# number_of_buildings and x and y coordinates called "xcoord_p" and "ycoord_p".
# It's used for filtering parcels when assigning locations of agents in HH travel survey 
# (such as residence, work, school) to parcels.
# Hana Sevcikova, PSRC
# March 2021

library(data.table)
setwd("~/psrc/urbansim-baseyear-prep/hhs_persons/2018/estimation")

bld <- fread("imputed_buildings_lodes_match_20210302.csv") # latest buildings table
pcl <- fread("parcels_coords.csv") # parcels with attributes "parcel_id", "x_coord_sp", "y_coord_sp"

pbld <- bld[, .(residential_units = sum(residential_units), number_of_buildings = .N), by = .(parcel_id)]

setkey(pbld, "parcel_id")
setkey(pcl, "parcel_id")

pclm <- merge(pcl, pbld, all = TRUE)
pclm[is.na(residential_units), residential_units := 0]
pclm[is.na(number_of_buildings), number_of_buildings := 0]

setnames(pclm, c("x_coord_sp", "y_coord_sp"), c("xcoord_p", "ycoord_p"))
fwrite(pclm, "parcels_for_hh_survey.csv")
