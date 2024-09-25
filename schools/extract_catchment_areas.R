library(data.table)

setwd("~/psrc/urbansim-baseyear-prep/schools")
pcl18 <- readRDS("~/psrc/R/shinyserver/baseyear2018explorer/data/parcels.rds")

xwalk <- fread("parcel2018to2023.csv")

res <- merge(xwalk, pcl18[, .(parcel_id, elem_id, mschool_id, hschool_id)], all.x = TRUE,
             by.x = "parcel2018_id", by.y = "parcel_id", sort = FALSE)
res <- res[, .(parcel_id = parcel2023_id, elem_id, mschool_id, hschool_id)]

fwrite(res, file = "parcels_catchment_areas.csv")

pcl23 <- readRDS("~/psrc/R/shinyserver/baseyear2023explorer/data/parcels.rds")
res2 <- merge(pcl23, res, by = "parcel_id", all.x = TRUE)
saveRDS(res2, "parcels.rds")
