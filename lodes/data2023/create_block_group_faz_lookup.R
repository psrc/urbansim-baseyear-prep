# Create a crosswalk between block groups and FAZes.
# If a BG belongs to multiple FAZes, decide on the basis of number of buildings.
# If it is a draw, decide using the number of parcels.
# Hana Sevcikova, PSRC
# 2024-06-25

library(data.table)
setwd("/Users/hana/psrc/urbansim-baseyear-prep/lodes/data2023")

data.dir <- "."
imputation.dir <- "../../imputation/data2023"

bg.column <- "census_block_group_id"

pcl <- fread(file.path(imputation.dir, "parcels_prelim.csv"))

cb <- fread("census_blocks.csv")

# add census_block_group_id to parcels
if(!"census_block_group_id" %in% colnames(pcl))
    pcl[cb, census_block_group_id := i.census_block_group_id, on = "census_block_id"]


lookup <- pcl[faz_id > 0, .N, by = c(bg.column, "faz_id")]

e <- new.env()
load(file.path(data.dir, "parcels_bldg_bg_share.rda"), envir = e)

bld.shares <- e$pcl
bld.shares[pcl, faz_id := i.faz_id, on = "parcel_id"]
nbld <- bld.shares[, .(number_of_buildings = sum(number_of_buildings_pcl)), 
           by = c(bg.column, "faz_id")]
lookup[nbld, Nbld := i.number_of_buildings, on = c(bg.column, "faz_id")][is.na(Nbld), Nbld := 0]
lookup[, `:=`(is_bmax = Nbld == max(Nbld), is_pmax = N == max(N)), by = bg.column][, Nmax := sum(is_bmax), by = bg.column]
lookup[Nmax > 1, `:=`(Nbld = N, is_bmax = is_pmax)]

ulookup <- lookup[is_bmax == TRUE, c(bg.column, "faz_id"), with = FALSE]

fwrite(ulookup, file = "block_group_faz.csv")
