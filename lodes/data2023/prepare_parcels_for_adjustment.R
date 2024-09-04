library(data.table)

setwd("~/psrc/urbansim-baseyear-prep/lodes/data2023")

imputation.dir <- "../../imputation/data2023"

bldgs <- fread(file.path(imputation.dir, "buildings_imputed_phase3_lodes_20240826.csv"))
#pcl <- fread(file.path(imputation.dir, "parcels_prelim.csv"))
pcl <- fread(file.path(imputation.dir, "parcels.csv"))
cb <- fread("census_blocks.csv")

# add census_block_group_id to parcels
if(!"census_block_group_id" %in% colnames(pcl))
    pcl[cb, census_block_group_id := i.census_block_group_id, on = "census_block_id"]

pcl <- pcl[, .(parcel_id, census_block_group_id, city_id, acres = round(gross_sqft/43560))]

bldgs.pcl <- bldgs[, .(number_of_buildings_pcl = .N), by = .(parcel_id)]
pcl[bldgs.pcl, number_of_buildings_pcl := i.number_of_buildings_pcl, on = .(parcel_id)][is.na(number_of_buildings_pcl), number_of_buildings_pcl := 0]
bldgs.bgs <- bldgs[, .(number_of_buildings_bg = .N), by = .(census_block_group_id)]
pcl <- merge(pcl, bldgs.bgs, by = "census_block_group_id", all.x = TRUE)
pcl[is.na(number_of_buildings_bg), number_of_buildings_bg := 0]
pcl[, acres_bg := round(sum(acres),2), by = "census_block_group_id"]

#fwrite(pcl, "parcels_bldg_bg_share.csv")
save(pcl, file = "parcels_bldg_bg_share.rda")
