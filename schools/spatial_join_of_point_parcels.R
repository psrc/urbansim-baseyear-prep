# spacial join of two point parcel datasets using nearest feature

library(sf)
library(data.table)

# load 2018 parcel geometry
pclgeo18 <- readRDS("~/psrc/R/shinyserver/baseyear2018explorer/data/parcels_geo.rds")
sfobj18 <- st_as_sf(pclgeo18, coords=c("lon","lat"), crs=4326)

#plot(sfobj18[sample(1:nrow(pclgeo18), 30000), "parcel_id"])

# load 2023 parcel geometry
pclgeo23 <- readRDS("~/psrc/R/shinyserver/baseyear2023explorer/data/parcels_geo.rds")
sfobj23 <- st_as_sf(pclgeo23, coords=c("lon","lat"), crs=4326)

#ind <- sample(1:nrow(pclgeo23), 30000)
#inf <- st_nearest_feature(sfobj23[ind,], sfobj18)
#plot(st_geometry(sfobj23[ind,][1:1000,]))
#plot(st_geometry(sfobj18[inf,][1:1000,]), col = "red", add = TRUE)

# run the nearest feature, resulting in an index within sfobj18
nf.index <- st_nearest_feature(sfobj23, sfobj18)

joined <- data.table(parcel2023_id = unlist(st_drop_geometry(sfobj23)[, "parcel_id"]),
                     parcel2018_id = unlist(st_drop_geometry(sfobj18)[nf.index, "parcel_id"]))
fwrite(joined, file = "parcel2018to2023.csv")

#ind1 <- which(unlist(st_drop_geometry(sfobj23)[, "parcel_id"]) %in% joined[duplicated(parcel2018_id)][, parcel2023_id])
#ind2 <- which(unlist(st_drop_geometry(sfobj18)[, "parcel_id"]) %in% joined[duplicated(parcel2018_id)][, parcel2018_id])
#plot(st_geometry(sfobj23[ind1,]))
#plot(st_geometry(sfobj18[ind2,]), col = "red", add = TRUE)
