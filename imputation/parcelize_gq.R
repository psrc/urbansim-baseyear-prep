library(data.table)

data.year <- 2017
data.dir <- paste0("data", data.year)

# read OFM data joine with usim blocks and aggregate to blocks
source(file.path(data.dir, "read_hh_totals.R"))
hhtots <- hhtots[, .(HH = sum(HH), GQ = sum(GQ)), by = census_block_id]

# read parcel data
parcels <- fread(file.path(data.dir, "parcels.csv"), sep=',', header=TRUE)
setkey(parcels, parcel_id)

# reduce parcels to census blocks that have GQs
pcl <- parcels[census_block_id %in% hhtots[GQ > 0, census_block_id], .(parcel_id, land_use_type_id, census_block_id, parcel_sqft)]

# assign tiers
lut1 <- c(8, 11)
lut2 <- c(2, 7, 9, 23)
lut3 <- setdiff(unique(pcl$land_use_type_id), c(19, 22, 25, 28, 4, 5, 1, 6, lut1, lut2))
pcl[land_use_type_id %in% lut1, tier := 1]
pcl[land_use_type_id %in% lut2, tier := 2]
pcl[land_use_type_id %in% lut3, tier := 3]
pcl[is.na(tier), tier := 4]

# iterate over tiers
result <- NULL
blocks.to.process <- unique(hhtots[GQ > 0, census_block_id])
for(level in 1:4) {
  pclt <- pcl[tier == level & census_block_id %in% blocks.to.process]
  pclt <- pclt[hhtots, gq := i.GQ, on = "census_block_id"]
  pclt[, `:=`(N = .N, sum_sqft = sum(parcel_sqft)), by = census_block_id]
  pclt[, adj_gq := round(gq * parcel_sqft/sum_sqft)]
  # add sampled parcels for blocks where the difference is large or where sum(gq) is 0
  pclt[, adj_gq_sum := sum(adj_gq), by = census_block_id]
  smpl <- c()
  for(bl in unique(pclt[gq - adj_gq_sum > 1 | adj_gq_sum == 0, census_block_id])) {
    spclt <- pclt[census_block_id == bl]
    dif <- spclt[, dif := gq - adj_gq_sum]$dif[1]
    smpl <- c(smpl, sample(spclt$parcel_id, dif, replace = TRUE, prob = spclt$parcel_sqft))
  }
  if(length(smpl) > 0) {
    # increase GQ for sampled parcels
    tbl <- table(smpl)
    setkey(pclt, parcel_id)
    pclt[.(as.integer(names(tbl))), adj_gq := adj_gq + tbl]
  }
  result <- rbind(result, pclt[, .(parcel_id, census_block_id, adj_gq)])
  blocks.to.process <- setdiff(blocks.to.process, unique(pclt$census_block_id))
}

# rename column and remove 0s
setnames(result, "adj_gq", "GQ")
result <- result[GQ > 0]

fwrite(result, file = "parcel_gq.csv")
