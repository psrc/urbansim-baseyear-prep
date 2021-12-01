library(data.table)

# unrolled NHB jobs as created by unroll_lodes.R
nhb.job.file <- "jobs_blodes_AJ_nohb.csv"

# distribution file
hb.distr.file <- "faz_home_based.csv"

# block-geo distribution file
block.lookup.file <- "block_faz.csv"
geo.lookup.id <- "faz_id"

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
nhb.jobs.wrk <- merge(nhb.jobs, geo.lookup, by = "census_block_id")

sum.jobs <- nhb.jobs.wrk[, .N, by = c(geo.lookup.id, "sector_id")]
sum.jobs <- merge(sum.jobs, hb.distr, by = c(geo.lookup.id, "sector_id"))

nhb.jobs.wrk2 <- merge(nhb.jobs.wrk, sum.jobs, by = c(geo.lookup.id, "sector_id"))
nhb.jobs.wrk2[is.na(home_based_jobs), home_based_jobs := 0]
# sample home-based jobs
hb.jobs <- nhb.jobs.wrk2[, .SD[sample(.N, min(.N, home_based_jobs))], by = c(geo.lookup.id, "sector_id")]
nhb.jobs[job_id %in% hb.jobs$job_id, home_based_status := 1]
nhb.jobs <- merge(nhb.jobs, geo.lookup, by = "census_block_id")
# put columns into the original order
nhb.jobs <- nhb.jobs[, c(colnames.wrk, geo.lookup.id), with = FALSE]

# check marginals
marg <- nhb.jobs[, .N, by = .(home_based_status, sector_id)]
setorder(marg, "home_based_status", "sector_id")
nhb.jobs[, .N, by = home_based_status]

# attach the original suffices
# correct home_based_status to be i1 instead of b1
colnames(nhb.jobs) <- c(sub("b1", "i1", colnames.orig), paste0(geo.lookup.id, ":i4"))
fwrite(nhb.jobs, file = "jobs_final.csv")
