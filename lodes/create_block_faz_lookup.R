library(data.table)
setwd("/Users/hana/psrc/urbansim-baseyear-prep/lodes")
pcl <- fread("~/psrc/R/analysis/capacity/run151/2014/parcels.csv")

lookup <- unique(pcl[faz_id > 0,.(census_block_id, faz_id)])
ulookup <- unique(lookup, by = "census_block_id")

fwrite(ulookup, file = "block_faz.csv")
