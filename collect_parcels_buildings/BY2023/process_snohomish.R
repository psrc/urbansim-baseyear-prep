# Script to create parcels and buildings tables from Snohomish assessor data
# for the use in urbansim 
# It generates 3 tables: 
#    urbansim_parcels, urbansim_buildings, building_type_crosstab
#
# Hana Sevcikova, last update 09/03/2024
#

library(data.table)

county <- "Snohomish"
county.id <- 61

# path to the Assessor text files
#data.dir <- "\\AWS-MODEL10\urbansim data\Projects\2023_Baseyear\Assessor\Extracts\Snohomish\Urbansim_Processing"
data.dir <- "Snohomish_data" # Hana's local path
misc.data.dir <- "data" # path to the BY2023/data folder
write.result <- FALSE # it will overwrite the existing tables urbansim_parcels & urbansim_buildings

if(write.result) source("mysql_connection.R")

set.seed(1234)

############
# Functions
############
fill.zeros <- function(values, l = 14){
    prefix.length <- l - nchar(values)
    prefix <- sapply(prefix.length, function(x) if(is.na(x) || x == 0) "" else paste(rep(0, x), collapse = ""))
    return(paste0(prefix, values))
}


###############
# Load all data
###############

# parcels & MainData 
parcels.base <- fread(file.path(data.dir, "parcels23_snoh_base.csv")) # contains all parcels
parcels.full <- fread(file.path(data.dir, "MainData.txt"), # contains all parcels attributes
                      colClasses = c(parcel_number = "character"))
stacked <- fread(file.path(data.dir, "parcels23_snoh_stacked_pins.csv"),
                 colClasses = c(parcel_id = "character", new_parcel_id = "character"))

# exemptions
exemptions <- fread(file.path(data.dir, "Exemptions.txt"), colClasses = c(parcel_number = "character"))

# reclass tables
lu_reclass <- fread(file.path(misc.data.dir, "land_use_generic_reclass_2023.csv"))
bt_reclass <- fread(file.path(misc.data.dir, "building_use_generic_reclass_2023.csv"))

# buildings
raw_buildings <- fread(file.path(data.dir, "improvement.txt"), 
                       colClasses = c(PIN = "character"))

# RVs to remove
rvs <- fread(file.path(data.dir, "snoh_rv_units.csv"), 
             colClasses = c(pin23 = "character", pin23_orig = "character"))

# PUD data
pud <- fread(file.path(data.dir, "pud_points_joined.csv"), colClasses = c(PARCEL_ID = "character"))

# CoStar data
costar.all <- fread(file.path(data.dir, "CostarExport_Snoh_MF_subset.csv"),
                    colClasses = c(Parcel_Min = "character", Parcel_Max = "character"))

# Info about fake parcels and buildings (prepared by Mark)
# (not needed as it was solved via stacked parcels)
#fake_parcels <- fread(file.path(data.dir, "snohomish_fake_parcel_changes.csv"))
#fake_buildings <- fread(file.path(data.dir, "snohomish_fake_building_changes.csv"))


###################
# Process parcels
###################

cat("\nProcessing Snohomish parcels\n=========================\n")

# make column names lowercase
colnames(parcels.full) <- tolower(colnames(parcels.full))

# type change & creating new id column from the Snohomish-specific id
parcels.base[, `:=`(parcel_id = as.character(parcelid))]
parcels.full[, `:=`(parcel_number = as.character(parcel_number), parcel_id = as.character(parcel_number))]

# join with stacked parcels
cat("\nNumber of stacked parcels matched: ", nrow(stacked[new_parcel_id %in% parcels.base[, parcel_id]]),
    " out of ", nrow(stacked))
parcels.full[stacked, parcel_id := i.new_parcel_id, on = c(parcel_number = "parcel_id")]

# base parcels that are not in parcels.full (should be zero!)
cat("\nNumber of base parcels not found in MainData: ", nrow(parcels.base[! parcel_id %in% parcels.full[, parcel_id]]))

# get exemption info
parcels.full[, exemption := parcel_number %in% exemptions[, parcel_number]]
             
# assign land use code of the base parcel
parcels.full[, is_base := parcel_id == parcel_number]
parcels.full[parcels.full[is_base == TRUE], base_usecode := i.usecode, on = "parcel_id"]

# for parcels without a base parcel, use the first value from each parcel_id (currently, no such record)
parcels.full[is.na(base_usecode), base_usecode := usecode[1], by = "parcel_id"] 

# assign land use type to base parcels
parcels.base[unique(parcels.full[, .(parcel_id, base_usecode)]), 
             usecode := i.base_usecode, on = "parcel_id"]

# join with reclass table
parcels.base[lu_reclass[county_id == county.id], `:=`(
    land_use_type_id = i.land_use_type_id, county_use_code = i.county_land_use_code),
             on = c(usecode = "county_land_use_description")]

# assign no code id to "exploded" parcels
parcels.base[is.na(land_use_type_id) & grepl("uni$", parcel_id), land_use_type_id := 17]

cat("\nMatched", nrow(parcels.base[!is.na(land_use_type_id)]), "records with land use reclass table")
cat("\nUnmatched: ", nrow(parcels.base[is.na(land_use_type_id)]), "records.")
if(nrow(misslu <- parcels.base[is.na(land_use_type_id) & !is.na(usecode), .N, by = "usecode"]) > 0){
    cat("\nThe following land use codes were not found:\n")
    print(misslu[order(usecode)])
} else cat("\nAll land use codes matched.")

parcels.base[is.na(land_use_type_id), land_use_type_id := 0]

# aggregate land and improvement value
parcels.aggr <- parcels.full[, .(land_value = sum(mklnd, na.rm = TRUE), 
                                improvement_value = sum(mkimp, na.rm = TRUE),
                                total_value = sum(mkttl, na.rm = TRUE),
                                exemption = as.integer(any(exemption)),
                                N = .N), by = "parcel_id"]
# the same for the original pins (needed for disaggregating improvement value into buildings)
parcels.aggr.old <- parcels.full[, .(land_value = sum(mklnd, na.rm = TRUE), 
                                 improvement_value = sum(mkimp, na.rm = TRUE),
                                 total_value = sum(mkttl, na.rm = TRUE),
                                 exemption = as.integer(any(exemption)),
                                 N = .N), by = "parcel_number"]

# merge with base parcels
prep_parcels <- merge(parcels.base, parcels.aggr, by = "parcel_id", all.x = TRUE)


# construct final parcels 
# (if no additional columns or other cleaning needed then it's just a copy of prep_parcels)
parcels_final <- prep_parcels[, .(parcel_id = 1:nrow(prep_parcels), parcel_id_fips = parcel_id, 
                                  land_value, improvement_value, total_value, gross_sqft = gis_sqft,
                                  x_coord_sp = point_x, y_coord_sp = point_y, land_use_type_id, 
                                  use_code = county_use_code, exemption, county_id = county.id)]

cat("\nTotal all:", nrow(parcels_final), "parcels")

###################
# Process buildings
###################

cat("\n\nProcessing Snohomish buildings\n=========================\n")

# make column names lowercase
colnames(raw_buildings) <- tolower(colnames(raw_buildings))
colnames(pud) <- tolower(colnames(pud))
colnames(costar.all) <- tolower(colnames(costar.all))

# fill parcel id with leading zeros
raw_buildings[!grepl("uni$", pin), pin := fill.zeros(pin)]

buildings_tmp <- raw_buildings[, .(building_id = 1:nrow(raw_buildings),
                                    parcel_number_orig = as.character(pin), 
                                    imprtype, usecode, usedesc,
                                    bldgtype, stories, yrbuilt, finsize,
                                    numberrooms, numbedrms, propext)]

prep_buildings <- merge(buildings_tmp, stacked, by.x = "parcel_number_orig", by.y = "parcel_id", all.x = TRUE)
prep_buildings[, parcel_id := ifelse(is.na(new_parcel_id), as.character(parcel_number_orig), 
                                    as.character(new_parcel_id))]

# remove buildings that cannot be assigned to parcels
nbld <- nrow(prep_buildings)
prep_buildings <- prep_buildings[parcel_id %in% parcels_final[, parcel_id_fips]]
cat("\nDropped ", nbld - nrow(prep_buildings), " buildings due to missing parcels.")

# assign land use type
prep_buildings[parcels_final, land_use_type_id := i.land_use_type_id, on = c(parcel_id = "parcel_id_fips")]

# join with building reclass table to get building type
prep_buildings[bt_reclass[county_id == county.id], building_type_id := i.building_type_id, 
               on = c(usecode = "county_building_use_code")]

# Calculate improvement value proportionally to the sqft (using the original parcel pins)
prep_buildings[parcels.aggr.old, `:=`(total_improvement_value = i.improvement_value),
               on = c(parcel_number_orig = "parcel_number")]
prep_buildings[, `:=`(sqft_tmp = pmax(1, finsize, na.rm = TRUE))]
prep_buildings[, `:=`(total_sqft = sum(sqft_tmp), count = .N), by = "parcel_number_orig"]
prep_buildings[, `:=`(improvement_value = round(sqft_tmp/total_sqft * total_improvement_value))]

# remove RVs in three areas: Port Susan, Gold Bar, Lake Connor
nbld <- nrow(prep_buildings)
prep_buildings <- prep_buildings[! parcel_number_orig %in% rvs[, fill.zeros(pin23_orig)]]
cat("\n", nbld - nrow(prep_buildings), " RVs removed.\n")
               
# As there are no mobile homes in the reclass table,
# set building type to mobile home if on mobile home land use type and if residential use code
prep_buildings[land_use_type_id == 13 & 
                   usecode %in% c("1", "", "5", "2", "3", "APART", "0", "70", "11"), 
               building_type_id := 11]

# join with PUD data
pud[, parcel_id := fill.zeros(parcel_id)]
prep_buildings[pud[, .(units = sum(first_count_address)), by = "parcel_id"], 
               pud_units := i.units, on = "parcel_id"]

# join with costar data
costar.all[, parcel_id := parcel_min]
costar <- costar.all[building_type %in% c("", "Apartments") & parcel_id == parcel_max]
costar[, ave_sqft_per_unit := as.numeric(gsub(",", "", ave_sqft_per_unit))][, parcel_id := fill.zeros(parcel_id)]
costar.dusize <- costar[, .(ave_sqft_per_unit = mean(ave_sqft_per_unit, na.rm = TRUE),
                            ave_sqft_per_unit_comp = sum(sqft, na.rm = TRUE)/sum(number_units, na.rm = TRUE)),
                        by = "parcel_id"]
costar.dusize[, ave_sqft_per_unit_fin := ifelse(is.na(ave_sqft_per_unit), 
                                                pmin(4000, ave_sqft_per_unit_comp), ave_sqft_per_unit)]
costar.dusize <- costar.dusize[ave_sqft_per_unit_fin >= 250]


# impute residential units
prep_buildings[, residential_units := 0]
prep_buildings[building_type_id %in% c(19, 11), residential_units := 1] # SF, MH
prep_buildings[usecode == 2, residential_units := 2]
prep_buildings[usecode == 3, residential_units := 3]

# for usecode 4 set it between 4 and 6 depending on sqft, using 800sf/DU
idx <- with(prep_buildings, usecode == 4)
tmpb <- merge(prep_buildings[idx], costar.dusize[, .(parcel_id, ave_sqft_per_unit = ave_sqft_per_unit_fin)], by = c("parcel_id"))
tmpb[, residential_units := pmax(1, round(sqft_tmp/ave_sqft_per_unit))]
prep_buildings[tmpb, units_from_costar := i.residential_units, on = "building_id"]
prep_buildings[idx, residential_units_comp := pmax(4, pmin(round(sqft_tmp/800), 6))]
prep_buildings[idx, residential_units := ifelse(!is.na(units_from_costar), 
                                                units_from_costar, residential_units_comp
                                                )]


# usecode == 5 looks like each record is an individual unit
# use NumberRooms for units for single-record Apartment buildings
prep_buildings[usecode == "APART" & numberrooms > 0, 
               `:=`(residential_units = numberrooms, building_type_id = 12)]

# for the types above, set building type MF if not already set otherwise
prep_buildings[usecode %in% c(2, 3, 4, 5, "APART") & is.na(building_type_id),
               building_type_id := 12]

# remove apartment buildings with zero units where the parcel contains some non-zero apartments and the sqft per unit ratio supports it
prep_buildings[usecode == "APART", `:=`(totrooms = sum(numberrooms, na.rm = TRUE), apart_totsqft = sum(finsize, na.rm = TRUE), Napart = .N), 
               by = parcel_id][is.na(totrooms), totrooms := 0][is.na(apart_totsqft), apart_totsqft := 0]
prep_buildings[usecode == "APART" & totrooms > 0, `:=`(tot_apart_sqft_per_units = apart_totsqft/totrooms, 
                                                       sqft_per_unit = finsize/numberrooms)][
                                                           , `:=`(has_small_ap = any(sqft_per_unit < 600)), by = .(parcel_id)]
remove.bldgs <- prep_buildings[usecode == "APART" & totrooms > 0 & tot_apart_sqft_per_units < 1500 & numberrooms == 0 & has_small_ap]
remove.pcl.sum <- remove.bldgs[, .(size = sum(finsize), value = sum(improvement_value), N = .N), by = "parcel_id"]

nbld <- nrow(prep_buildings)
prep_buildings <- prep_buildings[!building_id %in% remove.bldgs[, building_id]]
# distribute sqft and improvement value from the removed buildings into the existing buildings
prep_buildings[remove.pcl.sum, `:=`(size_added = i.size, value_added = i.value, Nremoved = i.N), on = "parcel_id"]
prep_buildings[!is.na(size_added) & !is.na(Napart), `:=`(finsize = as.integer(round(finsize + size_added/(Napart - Nremoved))),
                                                          improvement_value = as.integer(round(improvement_value + value_added/(Napart - Nremoved))))]

cat("\n", nbld - nrow(prep_buildings), " building records integrated into another records on the same parcel.")

# Assert 1 unit per each Condo unit and set building type as Condo if not already set otherwise
prep_buildings[usecode %in% c(51, 52, 53, 61, 62), 
               `:=`(residential_units = 1, 
                    building_type_id = ifelse(is.na(building_type_id), 4, building_type_id))]

# remaining MF
idx <- with(prep_buildings, residential_units == 0 & usecode %in% c(44, 63, 70, 'APART'))
tmpb <- merge(prep_buildings[idx], costar.dusize[, .(parcel_id, ave_sqft_per_unit = ave_sqft_per_unit_fin)], by = c("parcel_id"))
tmpb[, residential_units := pmax(1, round(sqft_tmp/ave_sqft_per_unit))]
prep_buildings[tmpb, units_from_costar := i.residential_units, on = "building_id"]
prep_buildings[idx, `:=`(residential_units_comp = pmax(1, round(sqft_tmp/800)))]
prep_buildings[idx, residential_units := ifelse(!is.na(units_from_costar), 
                                                units_from_costar, residential_units_comp
                                                )]

# for these use codes, set building type MF if not already set otherwise
prep_buildings[usecode %in% c(44, 63, 70, 'APART')  & is.na(building_type_id),
                    building_type_id := 12]

cat("\nMatched", nrow(prep_buildings[!is.na(building_type_id)]), "records with building reclass table")
cat("\nUnmatched: ", nrow(prep_buildings[is.na(building_type_id)]), "records.")
if(nrow(missbt <- prep_buildings[is.na(building_type_id), .N, by = c("usecode", "usedesc")]) > 0){
    cat("\nThe following building codes were not found:\n")
    print(missbt[order(-N)])
} else cat("\nAll building use codes matched.")


# aggregate DUs for Condo buildings on the same parcel
prep_buildings_condo <- NULL
for(bt in c(4)){
    prep_buildings_condo <- rbind(prep_buildings_condo,
                               prep_buildings[building_type_id == bt,
                                    .(building_id = building_id[1], building_type_id = bt, gross_sqft = sum(finsize),
                                      sqft_per_unit = round(sum(finsize)/sum(residential_units)),
                                      year_built = round(mean(yrbuilt)),
                                      residential_units = sum(residential_units),
                                      non_residential_sqft = 0,
                                      improvement_value = sum(improvement_value), use_code = usecode[1],
                                      stories = NA), by = "parcel_id"]
                        )
}

# there are a few apartment records with one record for each floor, but the sqft is from the whole building
apa_floor <- prep_buildings[residential_units > 0 & usecode == "APART", 
                      .(N = .N, DU = sum(residential_units), same_sqft = all(finsize == finsize[1])), 
                      by = "parcel_id"][, DUpb := DU/N]
fix_apa_floor <- apa_floor[N > 3 & same_sqft == TRUE & DUpb > 10, parcel_id]
# to be removed buildings are all but the last record by parcel and building type 
remove_bld <- prep_buildings[parcel_id %in% fix_apa_floor, 
                        .(building_id = building_id[-1]), by = c("parcel_id", "building_type_id")] # this will also remove multiple records for garages

nbld <- nrow(prep_buildings)
prep_buildings <- prep_buildings[!building_id %in% remove_bld[, building_id]]
cat("\nDropped ", nbld - nrow(prep_buildings), " building records representing building floors.")


# remove condos for now (they will be added later)
prep_buildings <- prep_buildings[building_type_id != 4]

# COSTAR & PUD adjustments 
# =========================
# adjust to costar/pud records that we are sure about

sumDU <- prep_buildings[,sum(residential_units)]

prep_buildings[costar[, .(units = sum(number_units)), by = "parcel_id"], 
               costar_units := i.units, on = "parcel_id"]

prep_buildings[, `:=`(residential_units_before_adj = residential_units,
                      is_mf_residential = building_type_id %in% c(12), 
                      is_sf_residential = building_type_id %in% c(19, 11))][
                          , is_residential :=  is_mf_residential | is_sf_residential]

# get sum of DUs per parcel and costar and pud info
# use only records where pud is close to costar and thus, the DU count is most likely correct
# (accept max difference of one)
pclblds <- prep_buildings[, .(DUall = sum(residential_units), Nall = sum(is_residential),
                              DUmf = sum(residential_units * is_mf_residential),
                              Nmf = sum(is_mf_residential), DUsf = sum(is_sf_residential),
                              costar = mean(costar_units, na.rm = TRUE),
                              pud = mean(pud_units, na.rm = TRUE)
                              ), 
                          by = "parcel_id"][!is.na(pud) & !is.na(costar)]
pclblds <- pclblds[Nall > 0 & (abs(pud - costar) < 2 | DUmf > pmax(costar, pud))]

# select records where there is a mismatch between our estimates and costar/pud
adjpcl <- pclblds[DUmf != costar & DUmf != pud & DUall != costar & DUall != pud]

# adjust to the larger value between costar and pud
adjpcl[, control := ifelse(abs(pud - costar) > 1, pmax(pud, costar), costar)]

# track processed parcel records
processed <- rep(FALSE, nrow(adjpcl))

# parcels with one building
idx <- processed == FALSE & with(adjpcl, Nall == 1)
prep_buildings[adjpcl[idx], residential_units := ifelse(is_residential, i.control, residential_units), 
               on = "parcel_id"]
processed[idx] <- TRUE

# parcels with one MF building
idx <- processed == FALSE & with(adjpcl, Nmf == 1)
prep_buildings[adjpcl[idx], residential_units := ifelse(is_mf_residential, pmax(1, i.control - DUsf),
                                                        residential_units), 
               on = "parcel_id"]
processed[idx] <- TRUE

# process buildings that don't have a MF building
idx <- processed == FALSE & with(adjpcl, Nmf == 0)
idx2 <- idx & with(adjpcl, DUall < control) # only those where control is larger
for(pcl in adjpcl[idx2, parcel_id]){
    add <- adjpcl[parcel_id == pcl, control - DUall]
    bidx <- which(with(prep_buildings, parcel_id == pcl & building_type_id == 19)) # index of affected buildings
    if(add %% length(bidx) == 0){ # try to distribute equally
        prep_buildings[bidx, residential_units := residential_units + add/length(bidx)]
    } else { # sample
        sampled <- sample(bidx, add, replace = TRUE, prob = prep_buildings[bidx, residential_units])
        tsampled <- table(sampled)
        prep_buildings[as.integer(names(tsampled)), residential_units := residential_units + tsampled]
    }
}
processed[idx] <- TRUE

# process remaining buildings
idx <- processed == FALSE
for(pcl in adjpcl[idx, parcel_id]){
    bidx <- which(with(prep_buildings, parcel_id == pcl & is_mf_residential))
    dif <- adjpcl[parcel_id == pcl, control - DUall]
    sampled <- sample(bidx, abs(dif), replace = TRUE, prob = prep_buildings[bidx, residential_units])
    tsampled <- table(sampled)
    if(dif > 0){ # add units
        prep_buildings[as.integer(names(tsampled)), residential_units := residential_units + tsampled]
    } else { # remove 
        prep_buildings[as.integer(names(tsampled)), residential_units := pmax(1, residential_units - tsampled)]
    }
}

sumDUafter <- prep_buildings[,sum(residential_units)]

cat("\nAdjustments with costar/pud data yields a change of ",  sumDUafter - sumDU, "DUs.")

# mark records for exclusion from further consolidation
pclblds2 <- prep_buildings[, .(DUall = sum(residential_units), Nall = sum(is_residential),
                              DUmf = sum(residential_units * is_mf_residential),
                              Nmf = sum(is_mf_residential), DUsf = sum(is_sf_residential),
                              costar = mean(costar_units, na.rm = TRUE),
                              pud = mean(pud_units, na.rm = TRUE)
                            ), 
                            by = "parcel_id"][
                                DUmf > 0 & ((!is.na(costar) & DUmf <= costar + 1) | (
                                    !is.na(pud) & DUmf <= pud + 1))]
prep_buildings[parcel_id %in% c(pclblds[, parcel_id], pclblds2[, parcel_id]),
               exclude_from_consolidation := 1]
prep_buildings[is.na(exclude_from_consolidation), exclude_from_consolidation := 0]

# assemble columns for final buildings table by joining residential and non-res part
# TODO: for non-res buildings is gross_sqft the same as non_residential_sqft?
buildings_final <- rbind(
    prep_buildings[building_type_id %in% c(12, 19, 11)
    , .(building_id, parcel_id, building_type_id, gross_sqft = finsize, 
        sqft_per_unit = round(finsize/residential_units),
        year_built = yrbuilt, residential_units, non_residential_sqft = 0,
        improvement_value, use_code = usecode, stories, exclude_from_consolidation
        )],
    prep_buildings_condo,
    prep_buildings[! building_type_id %in% c(4, 12, 19, 11)
    , .(building_id, parcel_id, building_type_id, gross_sqft = finsize, 
        sqft_per_unit = 1, year_built = yrbuilt, residential_units = 0,
        non_residential_sqft = finsize, improvement_value, use_code = usecode, 
        stories, exclude_from_consolidation
        )], fill = TRUE
    )

buildings_final[, parcel_id_fips := parcel_id]
buildings_final[parcels_final, `:=`(parcel_id = i.parcel_id), on = "parcel_id_fips"][
    , parcel_id_fips := fill.zeros(parcel_id_fips)]

# column order
setcolorder(buildings_final, c("building_id", "parcel_id", "parcel_id_fips"))

# remove redundant columns from parcels_final and add leading zeroes to parcel_id_fips
parcels_final[, `:=`(improvement_value = NULL, total_value = NULL)][!grepl("uni$", parcel_id_fips), 
                     parcel_id_fips := fill.zeros(parcel_id_fips)]

# replace NAs with zeros
for(col in c("gross_sqft", "year_built", "non_residential_sqft")){
    buildings_final[[col]][is.na(buildings_final[[col]])] <- 0
}


# clean stories column
# check with buildings_final[, .N, by = "stories"]
buildings_final[stories %in% c("5353", "53", "053", "2022", "", "\x80P@", "-D@"), stories := NA]
buildings_final[stories %in% c("2.,0", "2+", "2..0", "2.-0", "2,0"), stories := "2"]
buildings_final[stories == "1,5", stories := "1.5"]
buildings_final[, stories := as.numeric(stories)]
buildings_final[!is.na(stories) & stories > 0, land_area := round(gross_sqft/stories)]

cat("\nTotal all: ", nrow(buildings_final), "buildings")

# generate building type crosstabs
bt.tab <- buildings_final[!is.na(parcel_id), .(N = .N, DU = sum(residential_units), 
                                                nonres_sqft = sum(non_residential_sqft)), 
                          by = .(use_code, building_type_id)]
bt.tab <- merge(bt.tab, bt_reclass[county_id == county.id, .(county_building_use_code, county_building_use_description, building_type_id, building_type_name)],
                by.x = c("use_code", "building_type_id"), by.y = c("county_building_use_code", "building_type_id"),
                all.x = TRUE)
setcolorder(bt.tab, c("use_code", "county_building_use_description", "building_type_id", "building_type_name"))


if(write.result){
    # write results
    fwrite(parcels_final, file = "urbansim_parcels_snohomish.csv")
    fwrite(buildings_final, file = "urbansim_buildings_snohomish.csv")
    db <- paste0(tolower(county), "_2023_parcel_baseyear")
    connection <- mysql.connection(db)
    dbWriteTable(connection, "urbansim_parcels", parcels_final, overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "urbansim_buildings", buildings_final, overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "building_type_crosstab", bt.tab, overwrite = TRUE, row.names = FALSE)
    DBI::dbDisconnect(connection)
}

## Output on 2024/9/3
##############################
# Processing Snohomish parcels
# =========================
#     
# Number of stacked parcels matched:  37449  out of  37449
# Number of base parcels not found in MainData:  4772
# Matched 275422 records with land use reclass table
# Unmatched:  30 records.
# The following land use codes were not found:
#     usecode N
# 1:         2
# 
# Total all: 275452 parcels
# 
# Processing Snohomish buildings
# =========================
#     
# Dropped  416  buildings due to missing parcels.
# 1439  RVs removed.
# 
# 88  building records integrated into another records on the same parcel.
# Matched 276011 records with building reclass table
# Unmatched:  9 records.
# The following building codes were not found:
#     usecode                    usedesc N
# 1: STGMAINT Storage - Maintenance Bldg 7
# 2: BKSTRSCH        Bookstore - Schools 1
# 3: TRUCKSTP                 Truck Stop 1
# 
# Dropped  39  building records representing building floors.
# Adjustments with costar/pud data yields a change of  -2325 DUs.
# Total all:  260701 buildings
