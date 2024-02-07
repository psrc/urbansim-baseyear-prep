library(data.table)
library(readxl)
setwd("~/psrc/urbansim-baseyear-prep/lodes")

adjust.uw <- FALSE # not needed anymore
adjust.amazon <- TRUE 
redistribute.negatives <- TRUE
adjust.by.qcew <- TRUE

data.dir <- "data"

lodes.input.file <- file.path(data.dir, 'adjlodes_2023_bg.csv') # 2022 LODES adjusted to 2023 
lodes <- fread(lodes.input.file, colClasses=c("numeric", "character", "numeric", "numeric")) # set block group to character and county to numeric

lodes.output.file <- file.path(data.dir, paste0('Lodes23adj', format(Sys.Date(), "%Y%m%d"), '.csv'))

set.seed(12345)

# function for trimming leading and trailing spaces
trim <- function (x) gsub("^\\s+|\\s+$", "", x)

#lodes$sector_id <- as.integer(lodes$sector_id)
lodes[, census_2020_block_group_id := trim(census_2020_block_group_id)]

lodes.county.totals <- lodes[, .(emp_lodes = sum(number_of_jobs)), by = "county_id"]

# adjust two UW BGs
if(adjust.uw){
    uw.change <- 24139
    if(nrow(lodes[census_2020_block_group_id == "530330053032" & sector_id == 13]) == 0)
        lodes <- rbind(lodes, data.table(county_id = 33, census_2020_block_group_id = "530330053032",
                                         sector_id = 13, number_of_jobs = 0))
    lodes[census_2020_block_group_id == "530330053031" & sector_id == 13, number_of_jobs := number_of_jobs - uw.change]
    lodes[census_2020_block_group_id == "530330053032" & sector_id == 13, number_of_jobs := number_of_jobs + uw.change]
    cat("\nUW block groups adjusted.")
}

# adjust Amazon
if(adjust.amazon){
    amazon.change <- 32400
    lodes[census_2020_block_group_id == "530330073031" & sector_id == 5, number_of_jobs := number_of_jobs - amazon.change]
    lodes[census_2020_block_group_id == "530330073031" & sector_id == 7, number_of_jobs := number_of_jobs + amazon.change]
    cat("\nAmazon sectors adjusted.")
}

# add ID
lodes[, id := 1:nrow(lodes)]

# Redistribute negatives
if(redistribute.negatives) {
    neg <- subset(lodes, number_of_jobs < 0)
    geo.hier <- list(block_group=12, tract=11, county=5, region=2)
    log.distr <- log.distr.counter <- rep(0, length(geo.hier))
    names(log.distr) <- names(log.distr.counter) <- names(geo.hier)
    
    for(i in 1:nrow(neg)) {
        to.remove <- abs(neg$number_of_jobs[i])
        lodes.neg.idx <- which(lodes$id == neg$id[i])
        # iterate over geographies
        for(geo in names(geo.hier)) {
            subs <- subset(lodes, substr(census_2020_block_group_id, 1, geo.hier[[geo]]) == substr(neg$census_2020_block_group_id[i], 1, geo.hier[[geo]]) & sector_id == neg$sector_id[i] & number_of_jobs > 0)
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
    
    logres <- data.frame(number_of_jobs=log.distr, number_of_block_groups=log.distr.counter)
    rownames(logres) <- names(log.distr)
    cat("\nNegatives redistributed as follows:\n")
    print(logres)
}

# Adjust by unsupressed QCEW city totals
if(adjust.by.qcew){
    adjutment.threshold <- 0.1 # determines which records should be adjusted
    lodesadj <- copy(lodes)
    # join lodes with US census block group id
    cbg <- fread(file.path(data.dir, "census_2020_block_groups.csv"), colClasses = c("numeric", "character", "numeric"))
    lodesadj[cbg, census_block_group_id := i.census_2020_block_group_id, 
             on = c(census_2020_block_group_id = "census_2020_block_group_geoid")]
    
    # load cities table
    allcities <- fread(file.path(data.dir, "cities.csv"))
    allcities[, acity_id := city_id]
    #allcities[city_id > 1000, acity_id := city_id - 1000]
    allcities <- rbind(allcities, data.table(acity_id = c(9991:9994, 9999), 
                                             city_name = c(rep("Unincorporated", 4), "Region"),
                                             county_id = c(33, 35, 53, 61, 99)), fill = TRUE)
    allcities[city_name == "Sea Tac", city_name := "SeaTac"]
    
    # check alignemnet of the cities
    #mcity <- merge(unique(allcities[, .(city_name, county_id, incity = 1)]), unique(qcew[, .(juris, county, inqcew = 1)]), by.x = c("city_name", "county_id"), by.y = c("juris", "county"), all = TRUE)
    #mcity[is.na(inqcew)]
    #mcity[is.na(incity)]

    # create a column for adjusted counts
    lodesadj[, number_of_jobs_adj := number_of_jobs]
    
    # read parcel file to get BG x city correspondence 
    # - the file should also have columns number_of_buildings_pcl (# buildings per parcel)
    #   and number_of_buildings_bg (# buildings per block group)
    load(file.path(data.dir, "parcels_bldg_bg_share.rda"))
    pcl[allcities, acity_id := i.acity_id, on = "city_id"]
    
    # compute the share of buildings per block group and number of cities the BG belongs to
    pcl[, share_bg := ifelse(number_of_buildings_bg20 > 0, 
                        number_of_buildings_pcl/number_of_buildings_bg20, 
                        acres/acres_bg20)]
    pcl[, ncity := length(unique(acity_id)), by = .(census_2020_block_group_id)]
    
    # read qcew
    qcew <- fread(file.path(data.dir, "alljobs_juris_2022.csv"))
    # order rows so that sector totals are at the end of each city, and the region is at the very end
    qcew[county == 0, county := 99]
    qcew[industry == 0, industry := 99]
    qcew[unique(allcities[, .(city_name, acity_id, county_id)]), city_id := i.acity_id, 
         on = c(juris = "city_name", county = "county_id")]
    qcew <- qcew[order(county, city_id, industry)]
    qcew[emp_all==-99, emp_all := NA]
    qcew[, emp_all := as.numeric(emp_all)]
    qcew.county.totals <- qcew[county != 99 & industry == 99, .(emp_qcew = sum(emp_all, na.rm = TRUE)), by = "county"]
    county.totals <- merge(lodes.county.totals, qcew.county.totals, by.x = "county_id", by.y = "county")
    county.totals <- rbind(county.totals, data.table(county_id = 99, 
                                                     emp_lodes = sum(county.totals$emp_lodes),
                                                     emp_qcew = qcew[county == 99 & industry == 99, emp_all]))
    county.totals[, factor := emp_lodes/emp_qcew]
    qcew[county.totals, emp_all23 := emp_all * i.factor, on = c(county = "county_id")]
    
    # cities not found in the qcew dataset
    uucities <- unique(allcities[! acity_id %in% qcew[, city_id] & acity_id < 9000, 
                                 .(acity_id, county_id, city_name)])[order(county_id, acity_id)]
    
    if(any(is.na(qcew[, city_id]))) {
        cat("\nNo match found for\n")
        print(qcew[is.na(city_id)])
    }
    
    # Loop over cities, create a list of BGs for each city:
    # Each element is a table of BGs with a factor of the portion of each BG in this city (measured on # buildings)
    bglist <- list()
    for(city in unique(qcew[, city_id])){
        if(is.na(city)) next
        if(city == 9999) { # whole region, i.e. all BGs
            bglist[[city]] <- data.table(census_block_group_id = unique(pcl[, census_2020_block_group_id]), factor = 1)
            next
        }
        if(city > 9990) { # unincorporated
            cityset <- uucities[county_id == allcities[acity_id == city, county_id], acity_id]
        } else cityset <- city
        bglist[[city]] <- data.table(census_block_group_id = unique(pcl[acity_id %in% cityset, census_2020_block_group_id]), factor = 1)
        for(bg in unique(pcl[acity_id %in% cityset & ncity > 1, census_2020_block_group_id]))  # the factor should be < 1 for BGs shared among multiple cities
            bglist[[city]][census_block_group_id == bg, factor := pcl[census_2020_block_group_id == bg, sum(share_bg), by = "acity_id"][acity_id %in% cityset, sum(V1)]]
    }
    
    # loop over rows in qcew
    for(row in seq_len(nrow(qcew))){
        city <- qcew[row, city_id]
        if(is.na(qcew[row, emp_all23]) || is.na(city)) next # ignore suppressed records
        #if(city == 24 & row == 282) stop("")
        # for the relevant BGs adjust # jobs by the factor
        this.lodes <- lodesadj[census_block_group_id %in% bglist[[city]][, census_block_group_id], ]
        this.lodes[bglist[[city]], `:=`(factor = i.factor, number_of_jobs_bgadj = number_of_jobs_adj * i.factor), 
                       on = "census_block_group_id"]

        sector <- qcew[row, industry]
        joinon <- "census_block_group_id"
        if(sector < 99) {
            this.lodes <- this.lodes[sector_id == sector]
            joinon <- c(joinon, "sector_id")
        }
        
        # get the lodes total
        lodes.total <- this.lodes[, sum(number_of_jobs_bgadj)]
        
        # get qcew total
        qcew.total <- qcew[row, emp_all23]

        # adjust if the difference is big enough
        dif <- qcew.total - lodes.total
        #if(city == 45 & row == 515) stop("")
        if(abs(dif) > adjutment.threshold * qcew.total & lodes.total == 0){ # if there are no jobs seed them with 1
            this.lodes[, number_of_jobs_bgadj := factor]
            lodes.total <- this.lodes[, sum(number_of_jobs_bgadj)]
            lodesadj[this.lodes, number_of_jobs_adj := i.number_of_jobs_bgadj, on = joinon]
            dif <- qcew.total - lodes.total
        }
        if(abs(dif) > adjutment.threshold * qcew.total) { # adjust
            this.lodes[, adjustment := pmax(-number_of_jobs_adj, number_of_jobs_bgadj/sum(number_of_jobs_bgadj) * dif)]
            if(any(is.na(this.lodes[, adjustment]))) stop("")
            lodesadj[this.lodes, number_of_jobs_adj := pmax(0, number_of_jobs_adj + i.adjustment), on = joinon]
        } else this.lodes[, adjustment := 0]
        qcew[row, first_adjustment := this.lodes[, sum(adjustment)]]
    }
    # Record results (need to iterate again, because adjusting totals changed again the sectors
    # that might have been already adjusted before)
    for(row in seq_len(nrow(qcew))){
        city <- qcew[row, city_id]
        if(is.na(city)) next
        this.lodes <- lodesadj[census_block_group_id %in% bglist[[city]][, census_block_group_id], ]
        this.lodes[bglist[[city]], `:=`(number_of_jobs_adj = number_of_jobs_adj * i.factor,
                                        number_of_jobs_orig = number_of_jobs * i.factor), 
                   on = "census_block_group_id"]
        sector <- qcew[row, industry]
        if(sector != 0) this.lodes <- this.lodes[sector_id == sector]
        
        qcew[row, `:=`(lodes = this.lodes[, sum(number_of_jobs_orig)], 
                        lodes_adj = this.lodes[, sum(number_of_jobs_adj)])]
    }
    qcew[, total_adjustment := lodes_adj - lodes]
    cat("\nAdjustments for ", qcew[first_adjustment > 0, .N], " QCEW records.")
    fwrite(qcew, paste0("cities_qcew_lodes23_adjustments", format(Sys.Date(), "%Y%m%d"), ".csv"))
    setnames(lodesadj, "number_of_jobs", "number_of_jobs_orig")
    setnames(lodesadj, "number_of_jobs_adj", "number_of_jobs")
    lodes <- copy(lodesadj)
    lodes[, id := NULL]
    lodes[, number_of_jobs := round(number_of_jobs)]
}

# write output
fwrite(lodes, lodes.output.file)
