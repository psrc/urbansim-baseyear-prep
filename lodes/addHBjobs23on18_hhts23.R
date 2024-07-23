# Script to re-classify non-home-based jobs into home-based
# based on given geography x sector distribution using the HHTS 2023
# Hana Sevcikova, PSRC
# July 23, 2024

setwd("~/psrc/urbansim-baseyear-prep/lodes")

library(data.table)
library(mipfp)

data.dir <- "data"

# unrolled NHB jobs as created by unroll_adjusted_lodes.R
nhb.job.file <- "jobs_nohb2023.csv"

save.as.csv <- TRUE
save.into.mysql <- TRUE
out.jobs.name <- paste0("jobs_preparcelized_",  
                             format(Sys.Date(), '%Y%m%d'))

# FAZ distribution file
hb.distr.file <- file.path(data.dir, "faz_home_based_2017_19.csv")

# HHTS marginals
distr.targets <- fread(file.path(data.dir, "hbjobs_district_targets.csv"))
sector.targets <- fread(file.path(data.dir, "hbjobs_sector_targets.csv"))
distrXwalk <- fread(file.path(data.dir, "zone_faz_district_corres.csv"))
FazDistrXwalk <- unique(distrXwalk[, .(faz_id, district_id)])

# block-geo distribution file
block.lookup.file <- file.path(data.dir, "block_group_2020_faz.csv")
geo.lookup.id <- "faz_id"
aggr.geo.lookup.id <- "district_id"
block.id.name <- "census_2020_block_group_id"

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
nhb.jobs.wrk[FazDistrXwalk, district_id := i.district_id, on = geo.lookup.id]
hb.distr[FazDistrXwalk, district_id := i.district_id, on = geo.lookup.id]

# prepare seed distribution for IPF
seed.distr <- hb.distr[, .(seed_jobs = sum(home_based_jobs)), by = c("sector_id", aggr.geo.lookup.id)]
seed.tbl <- dcast(seed.distr, district_id ~ sector_id, value.var = "seed_jobs")
seed.tbl[, `13` := 1]

seed.tbl.df <- data.frame(seed.tbl[, -1, with = FALSE])
colnames(seed.tbl.df) <- colnames(seed.tbl)[-1]
rownames(seed.tbl.df) <- seed.tbl$district_id

sector.targets[sector_id == 13, HHTS23_Sector_Targets := sum(distr.targets$HHTS23_District_Targets) - sum(sector.targets$HHTS23_Sector_Targets)]
targets <- list(sector.targets$HHTS23_Sector_Targets, distr.targets$HHTS23_District_Targets)

# generate join distribution of district x sector
join.distr <- Ipfp(seed.tbl.df, list(2, 1), targets)$x.hat

hb.distr.hhts <- melt(data.table(join.distr)[, district_id := rownames(join.distr)], id.vars = "district_id",
                      variable.name = "sector_id", value.name = "home_based_jobs", variable.factor = FALSE)
hb.distr.hhts[, `:=`(sector_id = as.integer(sector_id), district_id = as.integer(district_id))]

sum.jobs <- nhb.jobs.wrk[, .N, by = c(aggr.geo.lookup.id, "sector_id")]
sum.jobs <- merge(sum.jobs, hb.distr.hhts, by = c(aggr.geo.lookup.id, "sector_id"))

nhb.jobs.wrk2 <- merge(nhb.jobs.wrk, sum.jobs, by = c(aggr.geo.lookup.id, "sector_id"))
nhb.jobs.wrk2[is.na(home_based_jobs), home_based_jobs := 0]
# sample home-based jobs
hb.jobs <- nhb.jobs.wrk2[, .SD[sample(.N, min(.N, home_based_jobs))], by = c(aggr.geo.lookup.id, "sector_id")]
nhb.jobs[job_id %in% hb.jobs$job_id, home_based_status := 1]
nhb.jobs <- merge(nhb.jobs, geo.lookup, by = block.id.name)
nhb.jobs[FazDistrXwalk, district_id := i.district_id, on = geo.lookup.id]

# put columns into the original order
nhb.jobs <- nhb.jobs[, c(colnames.wrk, aggr.geo.lookup.id, geo.lookup.id), with = FALSE]

# check marginals and total
marg <- nhb.jobs[, .N, by = .(home_based_status, sector_id)]
marg[, tot := sum(N), by = "sector_id"]
marg[, perc := N/tot * 100]
marg[home_based_status == 1, sum(N)/sum(tot) * 100]

setorder(marg, "home_based_status", "sector_id")
nhb.jobs[, .N, by = home_based_status]

if(save.as.csv){
    file.out <- paste0(out.jobs.name, ".csv")
    # attach the original suffices
    # correct home_based_status to be i1 instead of b1
    jobs.to.save <- copy(nhb.jobs)
    colnames(jobs.to.save) <- c(sub("b1", "i1", colnames.orig), paste0(aggr.geo.lookup.id, ":i4"), paste0(geo.lookup.id, ":i4"))
    fwrite(jobs.to.save, file = file.out)
}
if(save.into.mysql) {
    db <- "psrc_2023on2018_parcel_baseyear"
    source("../collect_parcels_buildings/BY2023/mysql_connection.R")
    connection <- mysql.connection(db)
    dbWriteTable(connection, out.jobs.name, nhb.jobs, overwrite = TRUE, row.names = FALSE)
    DBI::dbDisconnect(connection)
}

# convert it from command line into Opus cache:
# python -m opus_core.tools.convert_table csv flt -d . -o /path/to/opus/cache/2023 -t jobs_table_name
