# This script will sample building_ids in a block based on block level estimates to produce a parcelized output.

library(data.table)
library(openxlsx)
# library(foreign)

# orig.ofm <- read.table(file.path(data.dir, "OFMPopHHblocks.csv"), sep=",", header=TRUE) %>% as.data.table
# orig.ofm[, lapply(.SD, sum), .SDcols = c("POP2017", "HHP2017", "HU2017", "OHU2017", "GQ2017")]
# ofmdbf <- read.dbf("J:/OtherData/OFM/SAEP/SAEP Extract_2017_10October03/ofm_saep.dbf") %>% as.data.table # total GQ2017 = 79,805

# data.dir <- "C:/Users/CLam/Desktop/urbansim-baseyear-prep/imputation/data2017"
setwd(data.dir)
source("read_hh_totals.R") # HH in hhtots is actually HU (for usim scripting purposes)

calc.total.pop <- 1 # 0 = no, 1 = yes. If yes, will parcelize GQ
attribute <- "HHP" # HHP = Household Pop

# aggregate to usim census_block_id
ofm <- read.table("OFMPopHHblocks.csv", sep=",", header=TRUE) %>% as.data.table
colnames(ofm)[grep("\\d{4}$", colnames(ofm))] <- str_extract(colnames(ofm)[grep("\\d{4}$", colnames(ofm))], "[[:alpha:]]+")
hhtots[ofm[, GEOID10 := as.character(GEOID10)], on = c("GEOID10"), `:=` (POP = i.POP, HHP = i.HHP, OHU = i.OHU)]
tots <- hhtots[, lapply(.SD, sum), .SDcols = c("HH", "POP", "HHP", "GQ", "OHU"), by = .(census_block_id)
               ][, lapply(.SD, round), .SDcols = c("HH", "POP", "HHP", "GQ", "OHU"), by = .(census_block_id)]
totfil <- tots[get(eval(attribute)) > 0, ]

bldg <- fread("buildings_matched_OFM2017_2018-11-26.csv")[, .(building_id, parcel_id, residential_units)]
prcl <- fread("parcels.csv")[, .(parcel_id, census_block_id)]
bldg[prcl, on = c("parcel_id"), census_block_id := i.census_block_id]

# for every block in filtered block list
blocks <- totfil[['census_block_id']]
df <- NULL
res <- c()
for (block in blocks) {
  cat('\rProgress ', round((match(block, blocks))/length(blocks)*100), '%')
  totattr <- totfil[census_block_id %in% block, ][[attribute]]
  hu <- bldg[census_block_id %in% block, ][['residential_units']] 
  bldgids <- bldg[census_block_id %in% block, ][['building_id']] # does block exist in bldgs table?
  if (length(bldgids) == 0) {
    next
  } else if (length(bldgids) == 1) {
    v <- rep(bldgids, totattr)
  } else {
    v <- sample(bldgids, totattr, replace = TRUE, prob = hu)
  }
  res <- c(res, v)
}

df <- data.table(building_id = res)
dt <- df[bldg, on = "building_id", parcel_id := i.parcel_id]

if (calc.total.pop == 1) {
  # read GQ/parcel_id file
  ofmgq <- read.xlsx("Parcel_GQ2017.xlsx") %>% as.data.table # total GQ2017 = 79,540
  setnames(ofmgq, c("PSRC_ID", grep("GQ", colnames(ofmgq), value = T)), c("parcel_id", "estimate"))
  ofmgq[, estimate := round(estimate)] # round GQ values # 75492
  gqres <- rep(ofmgq$parcel_id, ofmgq$estimate)
  gqdt <- data.table(building_id = 0, parcel_id = gqres)
  # aggregate, include parcel_id, rbind, .N
  dfgq <- rbindlist(list(dt, gqdt))
  dtcnt <- dfgq[, TOTPOP := .N, by = .(building_id, parcel_id)]
  dt2 <- unique(dtcnt)
} else {
  dt <- dt[, (attribute) := .N, by = .(building_id, parcel_id)]
  dt2 <- unique(dt)
}


