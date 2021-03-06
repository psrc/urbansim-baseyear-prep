library(magrittr)
library(data.table)

# load data
data.dir <- "data"
#lodes.file <- file.path(data.dir, 'AdjLodes_BY18rev.csv') # 2017 LODES adjusted to 2018
lodes.file <- file.path(data.dir, 'AdjLodes18.csv') # 2018 LODES
lodes <- fread(lodes.file, colClasses=c("numeric", "character", "numeric", "numeric")) # set block group to character and county to numeric
#lodes17 <- fread('AdjLodes_BY18rev.csv', colClasses=c("character", "numeric", "numeric", "numeric")) # set block group to character and county to numeric


# function for trimming leading and trailing spaces
trim <- function (x) gsub("^\\s+|\\s+$", "", x)

#lodes$sector_id <- as.integer(lodes$sector_id)
lodes[, census_2010_block_group_id := trim(census_2010_block_group_id)]


# adjust two UW BGs and Amazon
uw.change <- 24139
if(nrow(lodes[census_2010_block_group_id == "530330053023" & sector_id == 13]) == 0)
  lodes <- rbind(lodes, data.table(county_id = 33, census_2010_block_group_id = "530330053023",
                                   sector_id = 13, number_of_jobs = 0))
lodes[census_2010_block_group_id == "530330053022" & sector_id == 13, number_of_jobs := number_of_jobs - uw.change]
lodes[census_2010_block_group_id == "530330053023" & sector_id == 13, number_of_jobs := number_of_jobs + uw.change]

amazon.change <- 32400
lodes[census_2010_block_group_id == "530330073003" & sector_id == 5, number_of_jobs := number_of_jobs - amazon.change]
lodes[census_2010_block_group_id == "530330073003" & sector_id == 7, number_of_jobs := number_of_jobs + amazon.change]

# add ID
lodes[, id := 1:nrow(lodes)]

# deal with negatives
neg <- subset(lodes, number_of_jobs < 0)
geo.hier <- list(block_group=12, tract=11, county=5, region=2)
log.distr <- log.distr.counter <- rep(0, length(geo.hier))
names(log.distr) <- names(log.distr.counter) <- names(geo.hier)

for(i in 1:nrow(neg)) {
  to.remove <- abs(neg$number_of_jobs[i])
  lodes.neg.idx <- which(lodes$id == neg$id[i])
  # iterate over geographies
  for(geo in names(geo.hier)) {
    subs <- subset(lodes, substr(census_2010_block_group_id, 1, geo.hier[[geo]]) == substr(neg$census_2010_block_group_id[i], 1, geo.hier[[geo]]) & sector_id == neg$sector_id[i] & number_of_jobs > 0)
    if(nrow(subs) > 0) {
      ids.to.select <- rep(subs$id, subs$number_of_jobs)
      selected <- sample(1:length(ids.to.select), min(to.remove, sum(subs$number_of_jobs)))
      tab <- table(ids.to.select[selected])
      tab <- tab[order(as.integer(names(tab)))]
      lodes.idx <- which(lodes$id %in% names(tab))
      lodes[lodes.idx, number_of_jobs := number_of_jobs - tab]
      stab <- sum(tab)		
      lodes[lodes.neg.idx, number_of_jobs := number_of_jobs + stab]
      to.remove <- to.remove - stab
      log.distr[geo] <- log.distr[geo] + stab
      log.distr.counter[geo] <- log.distr.counter[geo] + 1
      if(to.remove <= 0) break
    }
  }
}

logres <- data.frame(number_of_jobs=log.distr, number_of_blocks=log.distr.counter)
rownames(logres) <- names(log.distr)
cat("\nNegatives redistributed as follows:\n")
print(logres)

# join with US census block group id
bg <- fread(file.path(data.dir, "census_block_groups.csv"), colClasses = c("character", "numeric", "numeric"))
lodes[bg, census_block_group_id := i.census_block_group_id, on = "census_2010_block_group_id"]

# unroll into individual jobs
cat("\n\nWriting jobs.csv ...")
lodes.pos <- subset(lodes, number_of_jobs > 0)
ublocks <- unique(lodes.pos$census_block_group_id) %>% na.omit
jobs <- NULL
for(bl in ublocks) {
  recs <- subset(lodes.pos, census_block_group_id==bl)
  jobs <- rbind(jobs, data.table(sector_id=rep(recs$sector_id, recs$number_of_jobs), 
                                  census_block_group_id=bl))
}
jobs[,`:=`(job_id = 1:nrow(jobs), home_based_status=0, building_id=-1)]
# reorder columns and attach type for Opus
jobs <- jobs[, .(job_id, sector_id, home_based_status, building_id, census_block_group_id)] 
colnames(jobs) <- c('job_id:i4', 'sector_id:i1', 'home_based_status:b1', 'building_id:i4', 'census_block_group_id:i4')
# save to disk
fwrite(jobs, "jobs.csv") 

cat(" done.\n")

# convert it from command line into Opus cache:
# python -m opus_core.tools.convert_table csv flt -d . -o /path/to/opus/cache/2017 -t jobs