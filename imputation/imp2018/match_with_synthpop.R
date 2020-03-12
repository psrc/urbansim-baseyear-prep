library(data.table)

data.year <- 2018 # data files will be taken from "data{data.year}"
data.dir <- file.path("..", paste0("data", data.year))

bld.imp <- fread(file.path(data.dir, "imputed_buildings.csv"))
#pcl <- fread(file.path(data.dir, 'parcels.csv'))
#bld.imp <- merge(bld.imp, pcl[, .(parcel_id, census_block_id)], by = "parcel_id")

synthh <- fread(file.path(data.dir, "summary_adjusted_urbansim_bg_id.csv"))
setnames(synthh, "unique_id_for_base_year", "census_block_group_id")
setnames(synthh, "num_hh_adjusted", "HH")

bydu <- bld.imp[, .(DU = sum(residential_units), DUorig = sum(residential_units_orig)), by = .(census_block_group_id)]

bydu <- merge(bydu, synthh[, .(census_block_group_id, HH)], by = "census_block_group_id", all = TRUE)

cb <- fread(file.path(data.dir, "census_blocks.csv"))
cbg <- unique(cb[, .(census_block_group_id, county_id)]) 
bydu[cbg, county_id := i.county_id, on = "census_block_group_id"]
bydu <- bydu[, .(county_id, census_block_group_id, DU, DUorig, HH)]

source(file.path(data.dir, "load_ofm.R")) 
aggofm <- ofm[, .(HUofm = sum(HU)), by = "census_block_group_id"]
bydu <- merge(bydu, aggofm, by = "census_block_group_id", all = TRUE)

for(attr in c("DU", "DUorig", "HH", "HUofm"))
    bydu[is.na(bydu[[attr]]), attr] <- 0

bydu[, difHH := DU - HH]
bydu[, difOFM := DU - HUofm]

#plot(difHH ~ difOFM, data= bydu)
#abline(0,1)
#plot(DU ~ HH, data= bydu)
#hist(bydu[HH > DU, HH - DU], breaks = 50)

#bydu[HH > DU, sum(difHH), by = county_id]
bydu[HH > DU, sum(difHH)]

head(bydu[HH > DU, ][order(difHH)], 15)

fwrite(bydu, file = "DU_vs_HH_vs_OFM.csv")
