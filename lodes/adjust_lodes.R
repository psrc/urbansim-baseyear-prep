library(data.table)
library(readxl)
setwd("~/psrc/urbansim-baseyear-prep/lodes")

adjust.uw <- TRUE
adjust.amazon <- TRUE 
redistribute.negatives <- TRUE
adjust.by.qcew <- TRUE

data.dir <- "data"

lodes.input.file <- file.path(data.dir, 'AdjLodes18.csv') # 2018 LODES
lodes <- fread(lodes.input.file, colClasses=c("numeric", "character", "numeric", "numeric")) # set block group to character and county to numeric

lodes.output.file <- file.path(data.dir, paste0('Lodes18adj', format(Sys.Date(), "%Y%m%d"), '.csv'))

# function for trimming leading and trailing spaces
trim <- function (x) gsub("^\\s+|\\s+$", "", x)

#lodes$sector_id <- as.integer(lodes$sector_id)
lodes[, census_2010_block_group_id := trim(census_2010_block_group_id)]


# adjust two UW BGs
if(adjust.uw){
    uw.change <- 24139
    if(nrow(lodes[census_2010_block_group_id == "530330053023" & sector_id == 13]) == 0)
        lodes <- rbind(lodes, data.table(county_id = 33, census_2010_block_group_id = "530330053023",
                                         sector_id = 13, number_of_jobs = 0))
    lodes[census_2010_block_group_id == "530330053022" & sector_id == 13, number_of_jobs := number_of_jobs - uw.change]
    lodes[census_2010_block_group_id == "530330053023" & sector_id == 13, number_of_jobs := number_of_jobs + uw.change]
    cat("\nUW block groups adjusted.")
}

# adjust Amazon
if(adjust.amazon){
    amazon.change <- 32400
    lodes[census_2010_block_group_id == "530330073003" & sector_id == 5, number_of_jobs := number_of_jobs - amazon.change]
    lodes[census_2010_block_group_id == "530330073003" & sector_id == 7, number_of_jobs := number_of_jobs + amazon.change]
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
    cbg <- fread(file.path(data.dir, "census_block_groups.csv"), colClasses = c("character", "numeric", "numeric"))
    lodesadj[cbg, census_block_group_id := i.census_block_group_id, on = "census_2010_block_group_id"]
    
    # load cities table
    allcities <- fread(file.path(data.dir, "cities.csv"))
    allcities[, acity_id := city_id]
    allcities[city_id > 1000, acity_id := city_id - 1000]
    allcities <- rbind(allcities, data.table(acity_id = c(9991:9994, 9999), 
                                             city_name = c(rep("Unincorporated", 4), "Region"),
                                             county_id = c(33, 35, 53, 61, 99)), fill = TRUE)
    
    # create a column for adjusted counts
    lodesadj[, number_of_jobs_adj := number_of_jobs]
    
    # read parcel file to get BG x city correspondence 
    # - the file should also have columns number_of_buildings_pcl (# buildings per parcel)
    #   and number_of_buildings_bg (# buildings per block group)
    #pcl <- fread(file.path(data.dir, "parcels_bldg_bg_share.csv")) 
    load(file.path(data.dir, "parcels_bldg_bg_share.rda"))
    pcl[allcities, acity_id := i.acity_id, on = "city_id"]
    
    # compute the share of buildings per block group and number of cities the BG belongs to
    pcl[, bldg_share_bg := number_of_buildings_pcl/number_of_buildings_bg]
    pcl[, ncity := length(unique(acity_id)), by = .(census_block_group_id)]
    
    # read qcew
    qcew <- fread(file.path(data.dir, "alljobs_lum_juris.csv"))
    # order rows so that sector totals are at the end of each city, and the region is at the very end
    qcew[county == 0, county := 99]
    qcew[industry == 0, industry := 99]
    qcew[unique(allcities[, .(city_name, acity_id, county_id)]), city_id := i.acity_id, 
         on = c(juris = "city_name", county = "county_id")]
    qcew <- qcew[order(county, city_id, industry)]
    qcew[emp_all==-99, emp_all := NA]
    qcew[, emp_all := as.numeric(emp_all)]
    
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
            bglist[[city]] <- data.table(census_block_group_id = unique(pcl[, census_block_group_id]), factor = 1)
            next
        }
        if(city > 9990) { # unincorporated
            cityset <- uucities[county_id == allcities[acity_id == city, county_id], acity_id]
        } else cityset <- city
        bglist[[city]] <- data.table(census_block_group_id = unique(pcl[acity_id %in% cityset, census_block_group_id]), factor = 1)
        for(bg in unique(pcl[acity_id %in% cityset & ncity > 1, census_block_group_id]))  # the factor should be < 1 for BGs shared among multiple cities
            bglist[[city]][census_block_group_id == bg, factor := pcl[census_block_group_id == bg, sum(bldg_share_bg), by = "acity_id"][acity_id %in% cityset, sum(V1)]]
    }
    
    # loop over rows in qcew
    for(row in seq_len(nrow(qcew))){
        city <- qcew[row, city_id]
        if(is.na(qcew[row, emp_all]) || is.na(city)) next # ignore suppressed records

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
        qcew.total <- qcew[row, emp_all]

        # adjust if the difference is big enough
        dif <- qcew.total - lodes.total
        if(abs(dif) > adjutment.threshold * qcew.total) { # adjust
            this.lodes[, adjustment := pmax(-number_of_jobs_adj, number_of_jobs_bgadj/sum(number_of_jobs_bgadj) * dif)]
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
    fwrite(qcew, "cities_lodes_adjustments.csv")
    setnames(lodesadj, "number_of_jobs", "number_of_jobs_orig")
    setnames(lodesadj, "number_of_jobs_adj", "number_of_jobs")
    lodes <- copy(lodesadj)
    lodes[, id := NULL]
    lodes[, number_of_jobs := round(number_of_jobs)]
}

# write output
fwrite(lodes, lodes.output.file)
