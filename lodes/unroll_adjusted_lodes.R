library(magrittr)
library(data.table)

# load data
data.dir <- "data"
lodes.file <- file.path(data.dir, 'Lodes23adj20240220.csv') # 2023 LODES
lodes <- fread(lodes.file, colClasses=c(census_2020_block_group_geoid = "character")) # set block group to character and county to numeric

if(! "census_2020_block_group_id" %in% names(lodes)) {
    bg <- fread(file.path(data.dir, "census_2020_block_groups.csv"), colClasses = c("character", "numeric", "numeric"))
    lodes[bg, census_2020_block_group_id := i.census_2020_block_group_id, on = "census_2020_block_group_id"]
}

# unroll into individual jobs
cat("\n\nWriting jobs.csv ...")
lodes.pos <- subset(lodes, number_of_jobs > 0)
ublocks <- unique(lodes.pos$census_2020_block_group_id) %>% na.omit
jobs <- NULL
for(bl in ublocks) {
    recs <- subset(lodes.pos, census_2020_block_group_id==bl)
    jobs <- rbind(jobs, data.table(sector_id=rep(recs$sector_id, recs$number_of_jobs), 
                                   census_2020_block_group_id=bl))
}
jobs[,`:=`(job_id = 1:nrow(jobs), home_based_status=0, building_id=-1)]
# reorder columns and attach type for Opus
jobs <- jobs[, .(job_id, sector_id, home_based_status, building_id, census_2020_block_group_id)] 
colnames(jobs) <- c('job_id:i4', 'sector_id:i4', 'home_based_status:i1', 'building_id:i4', 'census_2020_block_group_id:i4')
# save to disk
fwrite(jobs, "jobs_nohb2023.csv") 

cat(" done.\n")

# convert it from command line into Opus cache:
# python -m opus_core.tools.convert_table csv flt -d . -o /path/to/opus/cache/2017 -t jobs