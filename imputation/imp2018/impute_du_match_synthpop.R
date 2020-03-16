library(data.table)

data.year <- 2018 # data files will be taken from "data{data.year}"
data.dir <- file.path("..", paste0("data", data.year))

bld.imp <- fread(file.path(data.dir, "imputed_buildings.csv"))
pcl <- fread(file.path(data.dir, 'parcels.csv'))
#bld.imp <- merge(bld.imp, pcl[, .(parcel_id, census_block_id)], by = "parcel_id")

synthh <- fread(file.path(data.dir, "summary_adjusted_urbansim_bg_id.csv"))
setnames(synthh, "unique_id_for_base_year", "census_block_group_id")
setnames(synthh, "num_hh_adjusted", "HH")

allrestypes <- c(12, 4, 19, 11, 10)
reslutypes <- c(13, # mobile home
                14, # MF
                15, # condo
                21, # recreation
                24, # SF
                26, # vacant developable
                30 # mix use
)
mftypes <- c(12, 4, 30)

compute.bydu <- function(dt) {
    bydu <- dt[, .(DU = sum(residential_units), DUorig = sum(residential_units_orig),
                    N = .N,
                    Nres = sum(building_type_id %in% allrestypes),
                    Nmf = sum(building_type_id %in% mftypes)
                    ), by = .(census_block_group_id)]

    bydu <- merge(bydu, synthh[, .(census_block_group_id, HH)], by = "census_block_group_id", all = TRUE)

    for(attr in c("DU", "DUorig", "HH", "Nres", "N", "Nmf"))
        bydu[is.na(bydu[[attr]]), attr] <- 0

    bydu[, difHH := DU - HH]
    bydu
}

bydu <- compute.bydu(bld.imp)
negdt <- bydu[difHH < 0]
bld <- copy(bld.imp)
bld[, imp1_residential_units := imp_residential_units]
bld[, residential_units1 := residential_units]

# for parcels with no res buildings build new buildings where possible 
new.bldgs <- NULL
s <- subset(negdt, Nres == 0)
if(nrow(s) > 0) {
    cat('\n\n')
    for (i in 1:nrow(s)){
        cat('\rProgress ', round(i/nrow(s)*100), '%')
        id <- s$census_block_group_id[i]
        is_pcl <- with(pcl, census_block_group_id == id)
        pidx <- which(is_pcl & pcl$land_use_type_id %in% reslutypes)
        if(length(pidx) == 0) { # no residential or vacant LUT
            if(s$N[i] > 0) next # if there is a building don't do anything
            pidx <- which(is_pcl & pcl$plan_type_id != 1000) # take all developable parcels
            if(length(pidx) == 0)
                pidx <- which(is_pcl)  # take all parcels if there are no developable parcels
        }
        new <- pcl[pidx, .(parcel_id, census_block_group_id, county_id, land_use_type_id, parcel_sqft, land_value)]
        new[, `:=`(residential_units = 1, # seed the buildings with 1 DU
                   residential_units_orig = 0, 
                   non_residential_sqft = 0,
                   building_type_id = 12, # new buildings will all be MF
                   imp_residential_units = 1,
                   is_residential = 1,
                   building_type_id_orig = -1
                   )]
        new.bldgs <- rbind(new.bldgs, new)
    }
    cat('\nImputed ', nrow(new.bldgs), ' buildings for ', length(unique(new.bldgs$census_block_group_id)), 
        'block groups with no residential buildings.\n')
    print(new.bldgs[, .N, by = census_block_group_id])
    # new building ids
    start.id <- max(bld$building_id) + 1
    new.bldgs <- new.bldgs[, building_id := seq(start.id, start.id + nrow(new.bldgs) - 1)]
    # combine original buildings with the new ones
    bld <- rbind(bld, new.bldgs, fill = TRUE)
    for (attr in colnames(bld))
        bld[is.na(bld[[attr]]), attr] <- if(is.character(bld[[attr]])) "" else 0
    # recompute bydu
    bydu <- compute.bydu(bld)
    negdt <- bydu[difHH < 0]
}

imputed.du <- nrow(new.bldgs)
imputed.bld <- 0

# 1 building: place DUs independently of type of building
s <- subset(negdt, N == 1)
if(nrow(s) > 0) {
    cat('\n\n')
    for (i in 1:nrow(s)){
        cat('\rProgress ', round(i/nrow(s)*100), '%')
        id <- s$census_block_group_id[i]
        is_id <- with(bld, census_block_group_id == id)
        bld[is_id, residential_units := residential_units + s[i, -difHH]]
        bld[is_id, imp_residential_units := 1]
        imputed.du <- imputed.du + s[i, -difHH]
        imputed.bld <- imputed.bld + 1
    }
    cat('\nImputed ', imputed.du, ' units into ', imputed.bld, ' buildings for blocks with 1 building.')
}

# > 1 buildings & 1 multi-family building: place DUs into that MF building
last.imputed.du <- imputed.du
last.imputed.bld <- imputed.bld
s <- subset(negdt, N > 1 & Nmf == 1)
if(nrow(s) > 0) {
    cat('\n\n')
    for (i in 1:nrow(s)) {
        cat('\rProgress ', round(i/nrow(s)*100), '%')
        id <- s$census_block_group_id[i]
        is_id <- with(bld, census_block_group_id == id & building_type_id %in% mftypes)
        bld[is_id, residential_units := residential_units + s[i, -difHH]]
        bld[is_id, imp_residential_units := 1]
        imputed.du <- imputed.du + s[i, -difHH]
        imputed.bld <- imputed.bld + 1
    }
    cat('\nImputed ', imputed.du-last.imputed.du, ' units into ', imputed.bld-last.imputed.bld, ' buildings for blocks with 1 MF building.')
}

# > 1 multi-family buildings
set.seed(1234)
last.imputed.du <- imputed.du
last.imputed.bld <- imputed.bld
s <- subset(negdt, N > 1 & Nmf > 1)
if(nrow(s) > 0) {
    cat('\n\n')
    for (i in 1:nrow(s)){
        cat('\rProgress ', round(i/nrow(s)*100), '%')
        id <- s$census_block_group_id[i]
        is_id <- with(bld, census_block_group_id == id & building_type_id %in% mftypes)
        # distribute units across MF buildings
        imp.idx <- which(is_id)
        nimp <- length(imp.idx)
        sDUimp <- sum(bld[is_id, residential_units])
        probs <- if(sDUimp == 0) rep(1, length=nimp)/nimp else bld[is_id, residential_units]/sDUimp
        if(nimp == 1) {
            imp.idx <- rep(imp.idx,2) # sample interprets things differently if the first number is just one integer
            probs <- rep(probs,2)
        }
        sampled.idx <- sample(imp.idx, s[i, -difHH], replace=TRUE, prob = probs)
        tab <- table(sampled.idx)
        row.idx <- as.integer(names(tab))
        bld[row.idx, residential_units:= residential_units + tab]
        bld[row.idx, imp_residential_units := 1]
        imputed.du <- imputed.du + s[i, -difHH]
        imputed.bld <- imputed.bld + length(row.idx)
    }
    cat('\nImputed ', imputed.du-last.imputed.du, ' units into ', imputed.bld-last.imputed.bld, ' buildings for ', nrow(s), ' blocks with multiple MF buildings.')
}

# 0 multi-family buildings & > 0 residential (other residential type than MF)
last.imputed.du <- imputed.du
last.imputed.bld <- imputed.bld
s <- subset(negdt, N > 1 & Nmf == 0 & Nres > 0)
if(nrow(s) > 0) {
    cat('\n\n')
    for (i in 1:nrow(s)){
        cat('\rProgress ', round(i/nrow(s)*100), '%')
        id <- s$census_block_group_id[i]
        is_id <- with(bld, census_block_group_id == id & building_type_id %in% allrestypes)
        # distribute units across res buildings
        imp.idx <- which(is_id)
        nimp <- length(imp.idx)
        sDUimp <- sum(bld[is_id, residential_units])
        probs <- if(sDUimp == 0) rep(1, length=nimp)/nimp else bld[is_id, residential_units]/sDUimp
        if(nimp == 1) {
            imp.idx <- rep(imp.idx,2) # sample interprets things differently if the first number is just one integer
            probs <- rep(probs,2)
        }
        sampled.idx <- sample(imp.idx, s[i, -difHH], replace=TRUE, prob=probs)
        tab <- table(sampled.idx)
        row.idx <- as.integer(names(tab))
        bld[row.idx, residential_units:= residential_units + tab]
        bld[row.idx, imp_residential_units := 1]
        imputed.du <- imputed.du + s[i, -difHH]
        imputed.bld <- imputed.bld + length(row.idx)
    }
    cat('\nImputed ', imputed.du-last.imputed.du, ' units into ', imputed.bld-last.imputed.bld, ' buildings for ', nrow(s), 'blocks with non-MF residential buildings.\n')
}

# 0 residential buildings (only non-residential type)
last.imputed.du <- imputed.du
last.imputed.bld <- imputed.bld
s <- subset(negdt, N > 1 & Nres == 0)
if(nrow(s) > 0) {
    cat('\n\n')
    for (i in 1:nrow(s)){
        cat('\rProgress ', round(i/nrow(s)*100), '%')
        id <- s$census_block_group_id[i]
        is_id <- with(bld, census_block_group_id == id)
        # distribute units across buildings proportional to non-res sqft
        imp.idx <- which(is_id)
        nimp <- length(imp.idx)
        sSQimp <- sum(bld[is_id, non_residential_sqft])
        probs <- if(sSQimp == 0) rep(1, length=nimp)/nimp else bld[is_id, non_residential_sqft]/sSQimp # take non-res sqft as  a proxy for the size
        if(nimp == 1) {
            imp.idx <- rep(imp.idx,2) # sample interprets things differently if the first number is just one integer
            probs <- rep(probs,2)
        }
        sampled.idx <- sample(imp.idx, s[i, -difHH], replace=TRUE, prob=probs)
        tab <- table(sampled.idx)
        row.idx <- as.integer(names(tab))
        bld[row.idx, residential_units:= residential_units + tab]
        bld[row.idx, imp_residential_units := 1]
        imputed.du <- imputed.du + s[i, -difHH]
        imputed.bld <- imputed.bld + length(row.idx)
        if(length(row.idx) > 0) {
            bld[row.idx, "building_type_id"] <- 12 # set to MF residential
        }
    }
    cat('\nImputed ', imputed.du-last.imputed.du, ' units into ', imputed.bld-last.imputed.bld, ' buildings for ', nrow(s), 'blocks with non-residential buildings.\n')
}
cat('\nTotals: ', imputed.du, 'units, ', imputed.bld, ' buildings\n')

file.out <- file.path(data.dir, "imputed_buildings_ofm_match.csv")
fwrite(bld, file = file.out)
