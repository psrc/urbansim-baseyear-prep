library(data.table)
setwd("~/psrc/urbansim-baseyear-prep/hhs_persons/2018/estimation")

bld <- fread("imputed_buildings_lodes_match_20210302.csv")
pcl <- fread("parcels_coords.csv")

pbld <- bld[, .(residential_units = sum(residential_units), number_of_buildings = .N), by = .(parcel_id)]

setkey(pbld, "parcel_id")
setkey(pcl, "parcel_id")

pclm <- merge(pcl, pbld, all = TRUE)
pclm[is.na(residential_units), residential_units := 0]
pclm[is.na(number_of_buildings), number_of_buildings := 0]

setnames(pclm, c("x_coord_sp", "y_coord_sp"), c("xcoord_p", "ycoord_p"))
fwrite(pclm, "parcels_for_hh_survey.csv")
