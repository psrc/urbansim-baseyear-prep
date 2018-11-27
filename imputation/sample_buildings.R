# library(data.table)

# orig.ofm <- read.table(file.path(data.dir, "OFMPopHHblocks.csv"), sep=",", header=TRUE) %>% as.data.table
# orig.ofm[, lapply(.SD, sum), .SDcols = c("POP2017", "HHP2017", "HU2017", "OHU2017")]

data.dir <- "C:/Users/CLam/Desktop/urbansim-baseyear-prep/imputation/data2017"
setwd(data.dir)
source("read_hh_totals.R")
rm(df)

attribute <- "HHP" # POP = Total Population (includes GQ), HHP = Household Pop

# hhtots aggregate to usim census_block_id
ofm <- read.table(file.path(data.dir, "OFMPopHHblocks.csv"), sep=",", header=TRUE) %>% as.data.table
colnames(ofm)[grep("\\d{4}$", colnames(ofm))] <- str_extract(colnames(ofm)[grep("\\d{4}$", colnames(ofm))], "[[:alpha:]]+")

hhtots[ofm[, GEOID10 := as.character(GEOID10)], on = c("GEOID10"), `:=` (POP = i.POP, HHP = i.HHP, GQ = i.GQ)]
tots <- hhtots[, lapply(.SD, sum), .SDcols = c("HH", "POP", "HHP", "GQ"), by = .(census_block_id)
               ][, lapply(.SD, round), .SDcols = c("HH", "POP", "HHP", "GQ"), by = .(census_block_id)]
totfil <- tots[get(eval(attribute)) > 0, ]

bldg <- fread("buildings_matched_OFM2017_20181126.csv")[, .(building_id, parcel_id, residential_units)]
prcl <- fread("parcels.csv")[, .(parcel_id, census_block_id)]

bldg[prcl, on = c("parcel_id"), census_block_id := i.census_block_id]

# for every block in totfil
blocks <- totfil[['census_block_id']]
df <- NULL

for (block in blocks) {
  totattr <- totfil[census_block_id %in% block, ][[attribute]]
  hu <- bldg[census_block_id %in% block, ][['residential_units']] 
  bldgids <- bldg[census_block_id %in% block, ][['building_id']] # does block exist in bldgs table?
  if (length(bldgids) == 0) {
    next
  } else if (length(bldgids) == 1) {
    # rep bldgids & hu
    bldgidss <- rep(bldgids, 2) 
    probs <- rep(hu, 2)
    v <- sample(bldgidss, totattr, replace = TRUE, prob = probs)
    tbl <- data.table(building_id = v)
  } else {
    v <- sample(bldgids, totattr, replace = TRUE, prob = hu)
    tbl <- data.table(building_id = v)
  }
  ifelse(is.null(df), df <- tbl, df <- rbindlist(list(df, tbl)))
}

# calc .N, include parcel_id
dt <- df[bldg, on = "building_id", parcel_id := i.parcel_id][, (attribute) := .N, by = .(building_id, parcel_id)]
dt2 <- unique(dt)
