library(data.table)
library(readxl)
setwd("~/psrc/urbansim-baseyear-prep/lodes/data")

bldgs <- fread("../../imputation/data2018/imputed_buildings_lodes_match_20210302.csv")
pcl <- fread("parcels.csv")

pcl <- pcl[, .(parcel_id, census_block_group_id, census_2020_block_group_id, city_id, acres = round(parcel_sqft/43560))]
bldgs[pcl, `:=`(census_2020_block_group_id = i.census_2020_block_group_id, 
                census_block_group_id = i.census_block_group_id),
                on = "parcel_id"]
bldgs.pcl <- bldgs[, .(number_of_buildings_pcl = .N), by = .(parcel_id)]
pcl[bldgs.pcl, number_of_buildings_pcl := i.number_of_buildings_pcl, on = .(parcel_id)][is.na(number_of_buildings_pcl), number_of_buildings_pcl := 0]
bldgs.bgs <- bldgs[, .(number_of_buildings_bg = .N), by = .(census_block_group_id)]
bldgs.bgs20 <- bldgs[, .(number_of_buildings_bg20 = .N), by = .(census_2020_block_group_id)]
pcl <- merge(pcl, bldgs.bgs, by = "census_block_group_id", all.x = TRUE)
pcl <- merge(pcl, bldgs.bgs20, by = "census_2020_block_group_id", all.x = TRUE)
pcl[is.na(number_of_buildings_bg), number_of_buildings_bg := 0]
pcl[is.na(number_of_buildings_bg20), number_of_buildings_bg20 := 0]
pcl[, acres_bg := round(sum(acres),2), by = "census_block_group_id"]
pcl[, acres_bg20 := round(sum(acres),2), by = "census_2020_block_group_id"]

#pcl[, bldg_share_bg := number_of_buildings_pcl/number_of_buildings_bg]
#pcl[share_bg > 0, ncity := length(unique(city_id)), by = .(census_block_group_id)]

#fwrite(pcl, "parcels_bldg_bg_share.csv")
save(pcl, file = "parcels_bldg_bg_share.rda")