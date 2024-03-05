library(data.table)

setwd("~/psrc/urbansim-baseyear-prep/imputation/imp2023")
save.as.csv <- TRUE
save.into.mysql <- TRUE
out.buildings.name <- paste0("buildings_imputed_phase3_lodes_", format(Sys.Date(), '%Y%m%d'))

gov.bts <- c(2, 5, 7, 9, 18)
sch.bts <- 18
restypes <- c(12, 4, 19, 11)
reslutypes <- c(13, # mobile home
                14, # MF
                15, # condo
                24 # SF
                )
publutypes <- c(2, 7, 9, 11, 23)
schlutypes <- 23

# load buildings
bld.file.name <- "buildings_imputed_phase2_ofm_20240305.csv"
data.year <- 2023 # data files will be taken from "../data{data.year}"
data.dir <- file.path("..", paste0("data", data.year))

bld <- fread(file.path(data.dir, bld.file.name))
pcl <- fread(file.path(data.dir, 'ref2018', 'parcels.csv'))

cbg <- fread(file.path(data.dir, "census_2020_block_groups.csv"))

# load qcew jobs data
jobs <- fread(file.path(data.dir, "blockgroup_public_employers.csv"))
setnames(jobs, "blockgroup_geoid", "census_2020_block_group_geoid")
setnames(jobs, "industry_id", "sector_id")

jobs[cbg, census_2020_block_group_id := i.census_2020_block_group_id, on = "census_2020_block_group_geoid"]
bgjobs <- jobs[, .(number_of_jobs = sum(number_of_jobs)), by = .(census_2020_block_group_id)]

public <- jobs[sector_id %in% c(12,13), .(Npub = sum(number_of_jobs)
                                            ), by = .(census_2020_block_group_id)]
edu <- jobs[sector_id == 13, .(Nedu = sum(number_of_jobs)), by = .(census_2020_block_group_id)]
bgjobs[public, Npub := i.Npub, on = "census_2020_block_group_id"][edu, Nedu := i.Nedu, on = "census_2020_block_group_id"]
bgjobs[is.na(Npub), Npub := 0]
bgjobs[is.na(Nedu), Nedu := 0]
bgjobs[, Npriv := number_of_jobs - Npub] 

# load Lodes data
lodes.file.name <- "Lodes23adj20240206.csv"
ljobs <- fread(file.path("../../lodes", lodes.file.name))
#colnames(ljobs) <- gsub(":.*", "", colnames(ljobs)) # remove column types from the names
setnames(ljobs, "census_2020_block_group_id", "census_2020_block_group_geoid")
ljobs[cbg, census_2020_block_group_id := i.census_2020_block_group_id, on = "census_2020_block_group_geoid"]
#ljobs[, number_of_jobs := 1]
lbgjobs <- ljobs[, .(number_of_jobs = sum(number_of_jobs)), by = .(census_2020_block_group_id)]
lpublic <- ljobs[sector_id %in% c(12,13), .(Npub = sum(number_of_jobs)), by = .(census_2020_block_group_id)]
ledu <- ljobs[sector_id == 13, .(Nedu = sum(number_of_jobs)), by = .(census_2020_block_group_id)]
lbgjobs[lpublic, Npub := i.Npub, on = "census_2020_block_group_id"][ledu, Nedu := i.Nedu, on = "census_2020_block_group_id"]
lbgjobs[is.na(Npub), Npub := 0]
lbgjobs[is.na(Nedu), Nedu := 0]

bgjobs[lbgjobs, `:=`(lodes = i.number_of_jobs, lodes_pub = i.Npub,
                     lodes_edu = i.Nedu), on = "census_2020_block_group_id"]

# aggregation function
aggregate.bld <- function(dt) {
    # aggregate
    blds <- dt[, .(NRSQ = sum(non_residential_sqft), 
                   Ngov = sum(building_type_id %in% gov.bts),
                   Nsch = sum(building_type_id %in% sch.bts),
                   Nnonres = sum(!building_type_id %in% restypes),
                   N = .N
    ), by = census_2020_block_group_id]
    # join with Lodes
    blds <- merge(blds, bgjobs, by = "census_2020_block_group_id", all = TRUE)
    # remove NAs
    for(attr in colnames(blds))
        blds[is.na(blds[[attr]]), attr] <- 0
    blds
}

bld.orig <- copy(bld)
aggdt <- aggregate.bld(bld)

set.seed(1234)

for(icond in 1:2) {
    if(icond == 1)
        s <- subset(aggdt, Nedu > 0 & Ngov == 0 & lodes_edu > 0) # Edu jobs available but no governmental building
    else
        s <- subset(aggdt, Npub-Nedu > 0 & Ngov-Nsch == 0 & lodes_pub-lodes_edu > 0) #other public jobs in no-school gov buildings
    new.bldgs <- NULL
    if(nrow(s) > 0) {
        for (i in 1:nrow(s)){
            id <- s$census_2020_block_group_id[i]
            # find a parcel to build a governmental building
            is_pcl <- with(pcl, census_2020_block_group_id == id)
            is_pub_pcl <- is_pcl & pcl$land_use_type_id %in% publutypes
            if(sum(is_pub_pcl) == 0) {
                is_nr_pcl <- is_pcl & ! pcl$land_use_type_id %in% reslutypes
                if(sum(is_nr_pcl) == 0) { # no non-residential or vacant LUT
                    pidx <- which(is_pcl & pcl$plan_type_id != 1000) # take all developable parcels
                    if(length(pidx) == 0)
                        pidx <- which(is_pcl)  # take all parcels if there are no developable parcels
                } else pidx <- which(is_nr_pcl)
            } else pidx <- which(is_pub_pcl)
            if(length(pidx) > 1) # we just need one parcel
                pidx <- sample(pidx, 1, prob = pcl[pidx, parcel_sqft + 1]) # sample proportionally to parcel_sqft
            new <- pcl[pidx, .(parcel_id, census_2020_block_group_id, county_id, land_use_type_id, parcel_sqft, land_value)]
            new[, `:=`(building_type_id = 5, # government
                       building_type_id_orig = -1
            )]
            new.bldgs <- rbind(new.bldgs, new)
        }
        # new building ids
        start.id <- max(bld$building_id) + 1
        new.bldgs <- new.bldgs[, building_id := seq(start.id, start.id + nrow(new.bldgs) - 1)]
        # combine original buildings with the new ones
        bld <- rbind(bld, new.bldgs, fill = TRUE)
        for (attr in colnames(bld))
            bld[is.na(bld[[attr]]), attr] <- if(is.character(bld[[attr]])) "" else 0
        # recompute aggregation
        aggdt <- aggregate.bld(bld)
    }
}

# clean year_built
bld[, .N, by = "year_built"][order(year_built)]
bld[(year_built < 1800) | (year_built > 2023), year_built := 0]

file.out <- file.path(data.dir, paste0(out.buildings.name, ".csv"))
if(save.as.csv) {
    # write out resulting buildings
    fwrite(bld, file=file.out)
}

if(save.into.mysql) {
    source("../../collect_parcels_buildings/BY2023/mysql_connection.R")
    db <- "psrc_2023_parcel_baseyear"
    connection <- mysql.connection(db)
    dbWriteTable(connection, out.buildings.name, bld, overwrite = TRUE, row.names = FALSE)
    DBI::dbDisconnect(connection)
}

cat("\nImputed", nrow(bld) - nrow(bld.orig), "governmental buildings.\n")
