# Create a crosswalk between block groups and FAZes.
# If a BG belongs to multiple FAZes, decide on the basis of number of buildings.
# If it is a draw, decide using the number of parcels.
# Hana Sevcikova, PSRC
# 2021-12-04

library(data.table)
setwd("/Users/hana/psrc/urbansim-baseyear-prep/lodes")
pcl <- fread("data/parcels.csv")

lookup <- pcl[faz_id > 0, .N, by = .(census_block_group_id, faz_id)]

e <- new.env()
load("data/parcels_bldg_bg_share.rda", envir = e)
bld.shares <- e$pcl
bld.shares[pcl, faz_id := i.faz_id, on = "parcel_id"]
nbld <- bld.shares[, .(number_of_buildings = sum(number_of_buildings_pcl)), 
           by = .(census_block_group_id, faz_id)]
lookup[nbld, Nbld := i.number_of_buildings, on = .(census_block_group_id, faz_id)][is.na(Nbld), Nbld := 0]
lookup[, `:=`(is_bmax = Nbld == max(Nbld), is_pmax = N == max(N)), by = .(census_block_group_id)][, Nmax := sum(is_bmax), by = .(census_block_group_id)]
lookup[Nmax > 1, `:=`(Nbld = N, is_bmax = is_pmax)]

ulookup <- lookup[is_bmax == TRUE, .(census_block_group_id, faz_id)]

fwrite(ulookup, file = "block_group_faz.csv")
