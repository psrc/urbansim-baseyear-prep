# Script to re-classify non-home-based jobs into home-based
# based on given geography x sector distribution
# Hana Sevcikova, PSRC
# August 27, 2024

setwd("~/psrc/urbansim-baseyear-prep/lodes")

library(data.table)

data.dir <- "data"
data.dir.yearspec <- "data2023"

# unrolled NHB jobs as created by unroll_adjusted_lodes.R
nhb.job.file <- "jobs_nohb_2024-08-27.csv"

save.as.csv <- TRUE
save.into.mysql <- TRUE
out.jobs.name <- paste0("jobs_preparcelized_",  
                             format(Sys.Date(), '%Y%m%d'))

# distribution file
hb.distr.file <- file.path(data.dir, "faz_home_based_2017_19.csv")

# block-geo distribution file
block.lookup.file <- file.path(data.dir.yearspec, "block_group_faz.csv")
geo.lookup.id <- "faz_id"
block.id.name <- "census_block_group_id"

# read jobs table
nhb.jobs <- fread(nhb.job.file)

# remove suffices in colnames
colnames.orig <- colnames(nhb.jobs)
colnames.wrk <- tstrsplit(colnames(nhb.jobs), split = ":", keep = 1)[[1]]
colnames(nhb.jobs) <- colnames.wrk

# read geo distribution and geo lookup
hb.distr <- fread(hb.distr.file, quote = "'")
geo.lookup <- fread(block.lookup.file)

# merge jobs with geo and distr
nhb.jobs.wrk <- merge(nhb.jobs, geo.lookup, by = block.id.name)

sum.jobs <- nhb.jobs.wrk[, .N, by = c(geo.lookup.id, "sector_id")]
sum.jobs <- merge(sum.jobs, hb.distr, by = c(geo.lookup.id, "sector_id"))

nhb.jobs.wrk2 <- merge(nhb.jobs.wrk, sum.jobs, by = c(geo.lookup.id, "sector_id"))
nhb.jobs.wrk2[is.na(home_based_jobs), home_based_jobs := 0]
# sample home-based jobs
hb.jobs <- nhb.jobs.wrk2[, .SD[sample(.N, min(.N, home_based_jobs))], by = c(geo.lookup.id, "sector_id")]
nhb.jobs[job_id %in% hb.jobs$job_id, home_based_status := 1]
nhb.jobs <- merge(nhb.jobs, geo.lookup, by = block.id.name)
# put columns into the original order
nhb.jobs <- nhb.jobs[, c(colnames.wrk, geo.lookup.id), with = FALSE]

# check marginals
marg <- nhb.jobs[, .N, by = .(home_based_status, sector_id)]
setorder(marg, "home_based_status", "sector_id")
nhb.jobs[, .N, by = home_based_status][, tot := sum(N)][, perc := N/tot*100][]

if(save.as.csv){
    file.out <- paste0(out.jobs.name, ".csv")
    # attach the original suffices
    # correct home_based_status to be i1 instead of b1
    jobs.to.save <- copy(nhb.jobs)
    colnames(jobs.to.save) <- c(sub("b1", "i1", colnames.orig), paste0(geo.lookup.id, ":i4"))
    fwrite(jobs.to.save, file = file.out)
}
if(save.into.mysql) {
    db <- "psrc_2023_parcel_baseyear"
    source("../collect_parcels_buildings/BY2023/mysql_connection.R")
    connection <- mysql.connection(db)
    dbWriteTable(connection, out.jobs.name, nhb.jobs, overwrite = TRUE, row.names = FALSE)
    DBI::dbDisconnect(connection)
}

# convert it from command line into Opus cache:
# python -m opus_core.tools.convert_table csv flt -d . -o /path/to/opus/cache/2023 -t jobs_table_name
