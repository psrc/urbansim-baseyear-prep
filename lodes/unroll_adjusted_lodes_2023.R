library(magrittr)
library(data.table)

# load data
data.dir <- "data"
data.dir.yearspec <- "data2023"

lodes.file <- file.path(data.dir.yearspec, 'Lodes23adj20241104.csv') # 2023 LODES
lodes <- fread(lodes.file, colClasses=c(census_2020_block_group_geoid = "character")) # set block group to character and county to numeric
bg <- fread(file.path(data.dir.yearspec, "census_block_groups.csv"), 
            colClasses = c("numeric", "character", "numeric", "numeric"))

if(! "census_block_group_id" %in% names(lodes)) {
    lodes[bg, census_block_group_id := i.census_block_group_id, 
          on = c(census_2020_block_group_id = "census_block_group_id")]
}

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
colnames(jobs) <- c('job_id:i4', 'sector_id:i4', 'home_based_status:i1', 'building_id:i4', 'census_block_group_id:i4')
# save to disk
fwrite(jobs, paste0("jobs_nohb_", Sys.Date(), ".csv")) 

cat(" done.\n")

# convert it from command line into Opus cache:
# python -m opus_core.tools.convert_table csv flt -d . -o /path/to/opus/cache/2023 -t jobs