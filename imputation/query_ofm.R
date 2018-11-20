# OFM block level data cannot be published or distributed! Keep on local machine or network

library(data.table)
local.dir <- "C:/Users/CLam/Desktop/urbansim-baseyear-prep/imputation/data2017"
setwd(local.dir)

ofm.dir <- "J:/OtherData/OFM/SAEP/SAEP Extract_2017_10October03"
year <- "2017"

attributes <- c("POP", "HHP", "GQ", "HU", "OHU")
columns <- c("COUNTYFP10", "GEOID10", paste0(attributes, year))

raw <- readRDS(file.path(ofm.dir, "ofm_saep.rds")) %>% as.data.table

ofm <- raw[, ..columns]

write.csv(ofm, "OFMPopHHblocks.csv")
