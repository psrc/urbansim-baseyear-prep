# This script will sample building_ids in a block based on block level estimates to produce a parcelized output.

library(data.table)
library(openxlsx)
# library(foreign)


data.dir <- "C:/Users/CLam/Desktop/urbansim-baseyear-prep/imputation/data2017"
setwd(data.dir)
source("read_hh_totals.R") # HH in hhtots is actually HU (for usim scripting purposes)

calc.total.pop <- 1 # 0 = no, 1 = yes. If yes, will parcelize GQ
attribute <- "HHP" # HHP = Household Pop

# aggregate to usim census_block_id
ofm <- read.table("OFMPopHHblocks.csv", sep=",", header=TRUE) %>% as.data.table
colnames(ofm)[grep("\\d{4}$", colnames(ofm))] <- str_extract(colnames(ofm)[grep("\\d{4}$", colnames(ofm))], "[[:alpha:]]+")
hhtots[ofm[, GEOID10 := as.character(GEOID10)], on = c("GEOID10"), `:=` (POP = i.POP, HHP = i.HHP, OHU = i.OHU)]
tots <- hhtots[, lapply(.SD, sum), .SDcols = c("HH", "POP", "HHP", "GQ", "OHU"), by = .(census_block_id)
               ][, lapply(.SD, round), .SDcols = c("HH", "POP", "HHP", "GQ", "OHU"), by = .(census_block_id)] # rounded results, lower than orig ofm
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
  # ofmgq <- read.xlsx("Parcel_GQ2017.xlsx") %>% as.data.table # total GQ2017 = 79,540
  ofmgq <- fread("parcel_gq.csv")
  setnames(ofmgq, c(grep("GQ", colnames(ofmgq), value = T)), c("estimate"))
  # setnames(ofmgq, c("PSRC_ID", grep("GQ", colnames(ofmgq), value = T)), c("parcel_id", "estimate"))
  # ofmgq[, estimate := round(estimate)] # round GQ values # 75492
  gqres <- rep(ofmgq$parcel_id, ofmgq$estimate)
  gqdt <- data.table(building_id = 0, parcel_id = gqres)
  # aggregate, include parcel_id, rbind, .N
  dfgq <- rbindlist(list(dt, gqdt))
  dtcnt <- dfgq[, totpop := .N, by = .(building_id, parcel_id)]
  dt2 <- unique(dtcnt)
} else {
  dt <- dt[, (attribute) := .N, by = .(building_id, parcel_id)]
  dt2 <- unique(dt)
}

# write.csv(dt2, paste0("parcelize_totpop_2017", Sys.Date(), ".csv"), row.names = F)
write.xlsx(dt2, paste0("parcelize_totpop_2017", Sys.Date(), ".xlsx"))


# QC ----------------------------------------------------------------------

# orig.ofm <- read.table(file.path(data.dir, "OFMPopHHblocks.csv"), sep=",", header=TRUE) %>% as.data.table
# orig.ofm[, GEOID10 := as.character(GEOID10)]
# orig.ofm[, lapply(.SD, sum), .SDcols = c("POP2017", "HHP2017", "HU2017", "OHU2017", "GQ2017")]
# ofmdbf <- read.dbf("J:/OtherData/OFM/SAEP/SAEP Extract_2017_10October03/ofm_saep.dbf") %>% as.data.table # total GQ2017 = 79,805

hhp <- copy(dt)
gq <- copy(gqdt)

tabulate.count <- function(table) {
  atable <- copy(table)
  atable[, matched := .N, by = c("building_id", "parcel_id")]
  newtable <- unique(atable)
  newtable[, lapply(.SD, sum), .SDcols = c("matched"), by = c("parcel_id")]
}

join.to.parcels <- function(table) {
  atable <- copy(table)
  prcl <- fread("parcels.csv")
  atable[prcl, on = c("parcel_id"), census_block_id := i.census_block_id]
}

join.to.blocklu <- function(table) {
  atable <- copy(table)
  atable[block.lu, on = "census_block_id", GEOID10 := i.GEOID10]
}

summarise.by.geoid <- function(table) {
  atable <- copy(table)
  atable[, lapply(.SD, sum), .SDcols = c("matched"), by = c("GEOID10")]
}

summarise.by.cbi <- function(table) {
  atable <- copy(table)
  atable[, lapply(.SD, sum), .SDcols = c("matched"), by = c("census_block_id")]
}

create.match.inter.tables <- function() {
  mat <- purrr::map(list("hhp" = hhp, "gq" = gq), tabulate.count)
  matint <- purrr::map(mat, join.to.parcels)
  matint2 <- purrr::map(matint, summarise.by.cbi)
  matint2$hhp[tots, on = "census_block_id", intermediate := i.HHP]
  matint2$gq[tots, on = "census_block_id", intermediate := i.GQ]
  return(matint2)
}

create.match.raw.tables <- function() {
  orig.ofm <- read.table(file.path(data.dir, "OFMPopHHblocks.csv"), sep=",", header=TRUE) %>% as.data.table
  orig.ofm[, GEOID10 := as.character(GEOID10)]
  
  mat <- purrr::map(list("hhp" = hhp, "gq" = gq), tabulate.count)
  matp <- purrr::map(mat, join.to.parcels)
  matpgeo <- purrr::map(matp, join.to.blocklu)
  matgeo <- purrr::map(matpgeo, summarise.by.geoid) # rollup by GEOID10
  matgeo$hhp[orig.ofm, on = "GEOID10", raw := i.HHP2017]
  matgeo$gq[orig.ofm, on = "GEOID10", raw := i.GQ2017]
  return(matgeo)
}

create.inter.raw.tables <- function() {
  totstbl <- copy(tots)
  totstbl[block.lu, on = "census_block_id", GEOID10 := i.GEOID10]
  tt <- totstbl[, lapply(.SD, sum), .SDcols = c("HHP", "GQ"), by = .(GEOID10)]
  
  orig.ofm <- read.table(file.path(data.dir, "OFMPopHHblocks.csv"), sep=",", header=TRUE) %>% as.data.table
  cols <- colnames(orig.ofm)[grep("\\d{4}", colnames(orig.ofm))]
  newcols <- str_extract(cols, "[[:alpha:]]+")
  orig.ofm[, GEOID10 := as.character(GEOID10)]
  setnames(orig.ofm, cols, newcols)
  o <- orig.ofm[, .(GEOID10, ofm_HHP = HHP, ofm_GQ = GQ)]
  tt[o, on = "GEOID10", `:=` (ofm_HHP = i.ofm_HHP, ofm_GQ = i.ofm_GQ)]
}

matint <- create.match.inter.tables()
matraw <- create.match.raw.tables()
rawint <- create.inter.raw.tables()

par(mfrow = c(2, 3))

plot(matint$hhp$matched, matint$hhp$intermediate, main = "HHP by matched and PSRC blocks", xlab = "matched", ylab = "PSRC blocks",segments(x0=0,y0=0,x1=4000,y1=4000), col = c("blue"), cex = .5)
plot(rawint$HHP, rawint$ofm_HHP, main = "HHP by PSRC blocks and OFM blocks", xlab = "PSRC blocks", ylab = "OFM blocks",segments(x0=0,y0=0,x1=4000,y1=4000), col = c("blue"), cex = .5)
plot(matraw$hhp$matched, matraw$hhp$raw, main = "HHP by matched and OFM blocks", xlab = "matched", ylab = "OFM blocks",segments(x0=0,y0=0,x1=4000,y1=4000), col = c("blue"), cex = .5)

plot(matint$gq$matched, matint$gq$intermediate, main = "GQ by matched and PSRC blocks", xlab = "matched", ylab = "PSRC blocks",segments(x0=0,y0=0,x1=4000,y1=4000), col = c("blue"), cex = .5)
plot(rawint$GQ, rawint$ofm_GQ, main = "GQ by PSRC blocks and OFM blocks", xlab = "PSRC blocks", ylab = "OFM blocks",segments(x0=0,y0=0,x1=4000,y1=4000), col = c("blue"), cex = .5)
plot(matraw$gq$matched, matraw$gq$raw, main = "GQ by matched and OFM blocks", xlab = "matched", ylab = "OFM blocks",segments(x0=0,y0=0,x1=4000,y1=4000), col = c("blue"), cex = .5)


# maindt[prcl, on = c("parcel_id"), census_block_id := i.census_block_id]
# maindt[block.lu, on = c("census_block_id"), GEOID10 := i.GEOID10]
# matchpop <- maindt[, lapply(.SD, sum), .SDcols = c("TOTPOP"), by = .(GEOID10)]
# dtplot <- matchpop[orig.ofm, on = "GEOID10"][, .(GEOID10, matched_pop = TOTPOP, ofm_pop = POP2017)]
# plot(dtplot$matched_pop, dtplot$ofm_pop, c)

# afile <- prcl[block.lu, on = "census_block_id", GEOID10 := i.GEOID10][, .(parcel_id, census_tract_id, census_block_group_id, census_block_id, GEOID10)]
# write.csv(afile, "T:/2018December/christy/parcel_block_lookup.csv", row.names = F)
