# Hana Sevcikova, PSRC
# 02/10/2020
# The script imputes building_type_id, residential_units and non_residential_sqft (partly also improvement_value) using a set of regression models.
# building_type_id is imputed or updated mainly using land use type. For missing land use types a parcels table from 2014 
# is used to inform the current type. 
# All I/O files are in the directory "data{data.year}" where data.year should be set to correspond to the datasets.
# Inputs: 
#   file buildings_for_imputation.csv
# 		In mysql create a table buildings_for_imputation which is a selection of specific buildings attributes joint with some parcels attributes. Use this query:
# 		create table buildings_for_imputation select a.parcel_id, building_id, year_built, a.gross_sqft, non_residential_sqft, residential_units, sqft_per_unit, stories, building_type_id, improvement_value, a.county_id, land_use_type_id, parcel_sqft, tax_exempt, land_value, is_inside_urban_growth_boundary from buildings as a left join parcels as b on a.parcel_id=b.parcel_id;
# 		Then export the result into a csv file.
#   file data2014/parcels.csv: parcels table from 2014 base year
#   file parcel_lookup_2018_2014.csv: correspondence between 2018 and 2014 parcels. It has columns parcel_id, parcel_id_2014.
#     It can be created using the data2018/clean_pcl_correspondence.R script which postprocesses the table parcel_points_2018.
# Output: file imputed_buildings.csv
#		It has the missing values filled in as well as new columns indicating which records were imputed.


library(data.table)
data.year <- 2018 # data files will be taken from "../data{data.year}"
data.dir <- file.path("..", paste0("data", data.year))
impute.net.sqft <- FALSE

# read buildings and parcels tables
bld.file.name <- 'buildings_20200316_mhs.csv'
bld.raw <- fread(file.path(data.dir, bld.file.name))

pcl <- fread(file.path(data.dir, 'parcels.csv'))
bld.raw[pcl, `:=`(land_use_type_id = i.land_use_type_id, parcel_sqft = i.parcel_sqft, 
                  land_value = i.land_value), on = "parcel_id"]
pcl_id <- 'parcel_id'

# read 2014 parcels table and 2018->2014 correspondence table 
pcl2014.look <- fread(file.path(data.dir, "parcel_lookup_2018_2014.csv"))
pcl2014 <- fread(file.path("../data2014", "parcels.csv"))
pcl2014[, parcel_id_2014 := parcel_id]
# keep only land_use_type_id
pcl2014.look[pcl2014, land_use_type_id := i.land_use_type_id, on = "parcel_id_2014"]

# translator from land use type into building type
lut.bt.pairs <- list(
           '1' = 1, # agriculture 
					 '2' = 2, # civic
					 '3' = 3, # commercial
					 '4' = 8, # fisheries into industrial
					 '5' = 8, # forest harvestable into industrial
					 '6' = 8, # forest protected into industrial
					 '7' = 5, # government
					 '8' = 6, # group quaters
					 '9' = 7, # hospital
					 '10' = 8, # industrial
					 '11' = 9, # military 
					 '12' = 8, # mining into industrial
					 '13' = 11, # mobile home park
					 '14' = 12, # multi-family
					 '15' = 12,  # condo into MF
					 '17' = 23, # no code to Other
					 '18' = 13, # office
					 '19' = 15, # park 
					 '20' = 16, # parking
					 '21' = 17, # recreation
					 '22' = 23, # right of way into Other
					 '23' = 18, # school
					 '24' = 19, # single-family
					 '25' = 20, # tcu
					 '26' = 23, # vacant into Other
					 '27' = 23, # vacant into Other
					 '28' = 21, # warehousing
					 '29' = 15, # water into open space
					 '30' = 10  # mixed use					 
					 )
# building types that are not suppose to have residential units (they will be deleted)
no.unit.types <- c(17, # recreation
                   7,  # hospital
                   14, # outbuilding
                   16  # parking
                    )
no.unit.types.if.parcel.allows <- c(23 # Other
                    )

# keep the original residential_units colum
residential.bts <- c(19, 12, 4, 11)
bld.raw[, residential_units_orig := residential_units]
bld.raw[, non_residential_sqft_orig := non_residential_sqft]
#bld.raw[, .N, by = building_type_id][order(building_type_id)]
#bld.raw[, .N, by = land_use_type_id][order(land_use_type_id)]

to.impute.bts <- c(0, 22)

bld <- copy(bld.raw)
set.seed(1)

# Impute missing land use type from 2014 parcels (exclude no code and vacant)
########################
#pcl <- unique(bld.raw[, .(parcel_id, land_use_type_id)])
pcl[pcl2014.look, land_use_type_id_2014 := i.land_use_type_id, on = "parcel_id"]
impute <- pcl$land_use_type_id == 0 & !pcl$land_use_type_id_2014 %in% c(16, 17, 26, 27) & !is.na(pcl$land_use_type_id_2014)
pcl[impute, `:=`(imp_land_use_type_id = TRUE, land_use_type_id = land_use_type_id_2014)]
pcl[is.na(imp_land_use_type_id), imp_land_use_type_id := FALSE]
orig.lut <- copy(bld$land_use_type_id)
bld[pcl, `:=`(land_use_type_id_2014 = i.land_use_type_id_2014, land_use_type_id = i.land_use_type_id), on = "parcel_id"]


# Zero out units for specific building types
#############
du.before <- sum(bld$residential_units)
bld[building_type_id %in% no.unit.types & residential_units > 0, residential_units := 0]
bld[, DUpcl := sum(residential_units), by = parcel_id]
bld[building_type_id %in% no.unit.types.if.parcel.allows, DUpcl_notypes := sum(residential_units), 
    by = parcel_id][is.na(DUpcl_notypes), DUpcl_notypes := 0]
bld[building_type_id %in% no.unit.types.if.parcel.allows & residential_units > 0 & DUpcl > DUpcl_notypes, residential_units := 0]
bld[, `:=`(DUpcl = NULL, DUpcl_notypes = NULL)]
du.after <- sum(bld$residential_units)

cat('\n', du.before - du.after, 'DUs removed from unwanted building types')

# Zero out non-res sqft for some buildings in Kitsap county
sqft.before <- sum(bld$non_residential_sqft)
change <- with(bld, county_id == 35 & building_type_id %in% residential.bts & gross_sqft > 0 & gross_sqft == non_residential_sqft)
bld[change, non_residential_sqft := 0]
sqft.after <- sum(bld$non_residential_sqft)

cat('\n', sqft.before - sqft.after, 'non-res sqft removed from Kitsap county')

# set res/non-res flags 
bld[, is_mixuse := residential_units > 0 & non_residential_sqft > 0]
bld[, is_residential := residential_units > 0 & !is_mixuse]
bld[, is_non_residential := non_residential_sqft > 0 & !is_mixuse]

# special cases (Mark's query)
#bld[is_residential == TRUE & building_type_id %in% residential.bts & gross_sqft > 0 & improvement_value > 1000 & improvement_value/gross_sqft < 30 & residential_units > 9 & sqft_per_unit > 1 & sqft_per_unit < 400]
#bld[parcel_id := 507079]


# Consolidate MF buildings
########################
consider <- with(bld, is_residential & building_type_id %in% c(12, 4) & !is.na(sqft_per_unit) & sqft_per_unit > 1)
bld[consider, max_du := max(residential_units), by = parcel_id]
bld[consider, is_max_du := residential_units == max_du]
bld[consider, Nmf := .N, by = parcel_id]
bld[consider, Nmf_max := sum(is_max_du), by = parcel_id]
bld[consider, Perc_mf_max := Nmf_max/Nmf * 100]
bld[consider & is_max_du, has_small_spu := any(sqft_per_unit < 300), by = parcel_id][is.na(has_small_spu), has_small_spu := FALSE]
consolidate <- consider & with(bld, Perc_mf_max > 49 & Nmf_max > 1 & has_small_spu == TRUE)
#bld[consolidate, max_year := max(year_built), by = parcel_id]
bld[, `:=`(max_du = NULL, is_max_du = NULL, Nmf = NULL, Nmf_max = NULL, Perc_mf_max = NULL,
           #max_year = NULL, 
           has_small_spu = NULL)]
du.before <- sum(bld[, residential_units])
du.before.county <- bld[, .(DU=sum(residential_units)), by = county_id]
# select one building per parcel which will represent the consolidated buildings
selected <- bld[consolidate, .SD[which.max(residential_units)], by = parcel_id]
# sum attributes for new buildings
new_bld <- bld[consolidate, .(gross_sqft = sum(gross_sqft), 
                              non_residential_sqft = sum(non_residential_sqft),
                              improvement_value = sum(improvement_value)), 
               by = parcel_id]
# delete the original attributes
for(attr in colnames(new_bld))
  if(attr != "parcel_id") selected[[attr]] <- NULL
# merge selected buildings with the summed attributes
new_bld <- merge(selected, new_bld)
# compute new sqft_per_unit
new_bld[, sqft_per_unit := as.integer(round(gross_sqft/residential_units))]
# remove consolidated buildings from the original dataset and add new buildings
bld <- bld[!consolidate]
bld <- rbind(bld, new_bld)
bld[building_id %in% selected$building_id, consolidated := TRUE]
du.after <- sum(bld[, residential_units])
du.after.county <- bld[, .(DU=sum(residential_units)), by = county_id]
du.dif <- du.before.county[du.after.county, .(county_id, DUremoved = DU - i.DU), on = "county_id"]
cat('\n', du.before - du.after, 'DUs removed due to buildings consolidation.\n')
print(du.dif)


# Consolidate non-res buildings in Pierce
########################
bld[is_non_residential & county_id == 53, Nbt := .N, by = .(parcel_id, building_type_id)]
consolidate <- with(bld, is_non_residential & !is.na(Nbt) & Nbt > 1)
bld[,Nbt := NULL]
# select one building per parcel and building type which will represent the consolidated buildings
selected <- bld[consolidate, .SD[which.max(non_residential_sqft )], by = .(parcel_id, building_type_id)]
# sum attributes for new buildings
new_bld <- bld[consolidate, .(gross_sqft = sum(gross_sqft), 
                              non_residential_sqft = sum(non_residential_sqft),
                              improvement_value = sum(improvement_value),
                              residential_units = sum(residential_units),
                              residential_units_orig = sum(residential_units_orig)), 
               by = .(parcel_id, building_type_id)]
# delete the original attributes
for(attr in colnames(new_bld))
    if(!attr %in% c("parcel_id", "building_type_id")) selected[[attr]] <- NULL
# merge selected buildings with the summed attributes
new_bld <- merge(selected, new_bld, by = c("parcel_id", "building_type_id"))
# remove consolidated buildings from the original dataset and add new buildings
nbld.before <- nrow(bld)
bld <- bld[!consolidate]
bld <- rbind(bld, new_bld)
bld[building_id %in% selected$building_id, consolidated := TRUE]
nbld.after <- nrow(bld)
cat('\n', nbld.before - nbld.after, 'non-res buildings removed in Pierce due to buildings consolidation.\n')


# Impute building types
########################
# impute/change building type for missing BT or for "no code"
bld <- bld[, building_type_id_orig := building_type_id]
# first set SF homes
bld[building_type_id %in% to.impute.bts & is_residential & residential_units <= 2, building_type_id := 19] # homes with 1 or 2 DUs
bld[building_type_id %in% to.impute.bts & is_residential & residential_units <= 4 & stories <=3 & stories > 0, building_type_id := 19]
# set the rest of residential buildings to MF
bld[building_type_id %in% to.impute.bts & is_residential, building_type_id := 12]
#bld[building_type_id == 0, .N, by = .(land_use_type_id, is_residential, is_mixuse)][order(land_use_type_id)]

# iterate over land use types
bt.before <- copy(bld$building_type_id)
nounits <- bld$residential_units == 0 & bld$non_residential_sqft == 0
is.nocode <- bld$building_type_id == 22
for(lut in names(lut.bt.pairs)) {
	is.lut <- bld$land_use_type_id==as.integer(lut)
	to.change <- is.lut & ( bld$building_type_id %in% to.impute.bts | (is.nocode & nounits))
	if(lut %in% c(13:15,24)) {# if residential LUT and ... 
		# ... non-res-sqft is zero then convert "no code"
	  to.change <- to.change | (is.lut & is.nocode & bld$non_residential_sqft == 0)
		# ... non-res BT (excl. mixed use and group quarters) and residential units are non-zero and non_residential_sqft is zero, then change BT
	  to.change <- to.change | (is.lut & !bld$building_type_id %in% c(10,6, residential.bts) & bld$is_residential)
	} else { # if non-residential LUT and ... 
	  if(! lut %in% c(16, 17, 26, 27)) # ... not no-code or vacant
	    # ... BT is residential but contains only non-res sqft
	    to.change <- to.change | (is.lut & bld$building_type_id %in% c(10, residential.bts) & bld$is_non_residential)
	}
	bld[to.change, building_type_id := lut.bt.pairs[[lut]]]
}

# reclassify records for LUT 30 that have res units and no non-res sqft into MF & SF
bld[land_use_type_id==30 & !building_type_id %in% c(10,6, residential.bts) & is_residential & residential_units <= 2, building_type_id := 19]
bld[land_use_type_id==30 & !building_type_id %in% c(10,6, residential.bts) & is_residential & residential_units > 2, building_type_id := 12]

# reclassify records with residential BT that only have non-residential sqft into commercial
bld[building_type_id %in% residential.bts & is_non_residential, building_type_id := 3]

cat('\nImputed ', sum(bld$building_type_id != bld$building_type_id_orig), ' values of building_type_id.')

bld[building_type_id_orig != building_type_id, .N, by = .(building_type_id_orig, building_type_id)][order(building_type_id_orig, building_type_id)]


#impute DU into single family
before.du <- sum(bld$residential_units)
imputed <- rep(FALSE, nrow(bld))
do.impute <- bld$building_type_id %in% c(19, 11) & bld$residential_units == 0 & !bld$is_non_residential
bld[do.impute, residential_units := 1]
imputed[do.impute] <- TRUE
bld <- bld[, imp_residential_units := imputed]
cat('\nImputed ', sum(bld$residential_units)- before.du, ' single-family residential units.')

#bldout <- copy(bld)
#bldout[, `:=`(is_mixuse = NULL, is_residential = NULL, is_non_residential = NULL, 
#              imp_residential_units = as.integer(imp_residential_units))]
#write.table(bldout, file=file.path(data.dir, "imputed_buildings_tmp.csv"), sep=',', row.names=FALSE)

for(attr in c('building_type_id', 'gross_sqft', 'improvement_value', 'land_use_type_id', 'parcel_sqft', 'stories', 'year_built', 'land_value', 'sqft_per_unit')) {
	if(attr %in% colnames(bld))
		bld[bld[[attr]]==0, attr] <- NA
}

is.res <- bld$building_type_id %in% residential.bts
is.nonres <- !is.res & !bld$building_type_id %in% c(6)
nounits <- bld$residential_units == 0 & bld$non_residential_sqft == 0
bld[is.res & sqft_per_unit == 1, sqft_per_unit := NA]
bld[residential_units == 0 &  is.res, residential_units :=  NA]
bld[non_residential_sqft == 0 &  is.nonres, non_residential_sqft := NA]
bld[nounits &  !is.na(building_type_id) & building_type_id %in% c(10), residential_units := NA] # mixed use
bld[nounits &  is.na(bld$building_type_id), `:=`(residential_units = NA, non_residential_sqft = NA)]

# special cases:
# King County: code 99 for missing values of stories
bld[!is.na(stories) & stories == 99 & county_id == 33, stories := NA]
# King County: code 1000 for missing values of improvement value
bld[!is.na(improvement_value) & improvement_value == 1000 & county_id == 33, improvement_value := NA]
# consider non_residential_sqft (and possibly net_sqft and gross_sqft) that equal to 1 as missing
bld[!is.na(non_residential_sqft) & is.nonres & non_residential_sqft == 1, non_residential_sqft := NA]
bld[!is.na(gross_sqft) & gross_sqft == 1, gross_sqft := NA]
# plot(improvement_value ~ non_residential_sqft, data = bld, log = "xy")

# pdf('nonres_sqft_vs_imprvalue.pdf')
# plot(improvement_value ~ non_residential_sqft, subset(bld, improvement_value>0 & non_residential_sqft > 0 & is.nonres), log="xy")
# dev.off()

# add column number_of_buildings
#dt <- copy(bld)
#tmp <- dt[, list(number_of_buildings=.N, number_of_mf_buildings=sum(building_type_id %in% c(12,14))), by=pcl_id]
#bld <- merge(bld, tmp, by=pcl_id)
is.res <- bld$building_type_id %in% residential.bts

# for residential buildings impute gross_sqft if sqft_per_unit is known
impute <- with(bld, is.res & is.na(gross_sqft) & !is.na(sqft_per_unit) & !is.na(residential_units))
bld[impute, gross_sqft := as.integer(sqft_per_unit * residential_units)]
bld[impute, imp_gross_sqft := TRUE]

# for residential buildings impute gross_sqft from improvement value
for(bt in c(12, 19, 11)) {
  lmfit <- lm(log(gross_sqft) ~ log(improvement_value), data = bld[building_type_id == bt])
  slmfit <- summary(lmfit)
  imp <- bld$building_type_id == bt & is.na(bld$gross_sqft) & !is.na(bld$improvement_value)
  lmpred <- predict(lmfit, bld[imp,]) + rnorm(sum(imp), 0, slmfit$sigma)
  bld[imp, gross_sqft := as.integer(round(exp(lmpred)))]
  bld[imp, imp_gross_sqft := TRUE]
}

# impute net_sqft for multi-family residential using linear regression 
if(impute.net.sqft) {
	ind <- with(bld, building_type_id %in% c(12, 4) & !is.na(net_sqft) & !is.na(gross_sqft) & net_sqft <= gross_sqft)
	bldres1 <- subset(bld, ind & !is.na(stories))
	bldres2 <- subset(bld, ind)
	# 1. records with non-missing stories
	lmfit1 <- lm(sqrt(net_sqft) ~ sqrt(gross_sqft) + stories, bldres1)
	slmfit1 <- summary(lmfit1)
	idx <- which(bld$building_type_id %in% c(12, 4) & is.na(bld$net_sqft) & !is.na(bld$gross_sqft) & !is.na(bld$stories) & is.na(bld$residential_units))
	lmpred <- predict(lmfit1, bld[idx,]) + rnorm(length(idx), 0, slmfit1$sigma)
	bld[idx,'net_sqft'] <- lmpred^2 # back-transform
	imputed <- rep(FALSE, nrow(bld))
	imputed[idx] <- TRUE
	bld <- cbind(bld, imp_net_sqft=imputed)
	cat('\nImputed ', length(idx), ' records of net_sqft for multi-family residential buildings using gross_sqft and stories.')
	# 2. records with missing stories
	lmfit2 <- lm(sqrt(net_sqft) ~ sqrt(gross_sqft), bldres2)
	slmfit2 <- summary(lmfit2)
	idx <- which(bld$building_type_id %in% c(12, 4) & is.na(bld$net_sqft) & !is.na(bld$gross_sqft) & is.na(bld$residential_units))
	lmpred <- predict(lmfit2, bld[idx,]) + rnorm(length(idx), 0, slmfit2$sigma)
	bld[idx,'net_sqft'] <- lmpred^2 # back-transform
	bld[idx, 'imp_net_sqft'] <- TRUE
	cat('\nImputed ', length(idx), ' records of net_sqft for multi-family residential buildings using gross_sqft.')
} else { # set net_sqft to gross_sqft
	bld[, net_sqft := gross_sqft]
}

# impute sqft_per_unit where gross_sqft known and non_residential_sqft is 0
bld[is.res & is.na(sqft_per_unit) & !is.na(gross_sqft) & !is.na(residential_units) & !is.na(non_residential_sqft) & non_residential_sqft == 0, 
    sqft_per_unit := as.integer(round(gross_sqft/residential_units))]
# set missing sqft_per_unit for MF residential records to 1000
bld[building_type_id %in% c(12, 4, 11) & is.na(sqft_per_unit), sqft_per_unit := 1000]
# set missing sqft_per_unit for SF residential records to 1800
bld[building_type_id %in% c(19) & is.na(sqft_per_unit), sqft_per_unit := 1800]


# impute residential units if net_sqft and sqft_per_unit is not missing
imp <- is.res & is.na(bld$residential_units) & ! is.na(bld$net_sqft) & bld$sqft_per_unit > 0
bld[imp, residential_units := as.integer(pmax(round(net_sqft/sqft_per_unit), 1))]
bld[imp, imp_residential_units := TRUE]
cat('\nImputed ', sum(bld[imp, residential_units]), '(', sum(imp), ' records) multi-family residential units as net_sqft/sqft_per_unit.')

# impute non_residential_sqft for non-residential buildings using linear regression
# bldnres <- subset(bld, !(building_type_id %in% c(19, 12, 4, 11, 6, 10)) & !is.na(non_residential_sqft) & !is.na(gross_sqft) & non_residential_sqft > 0)
# bldnres[, sqft_dif := gross_sqft - non_residential_sqft]
# lmfit <- lm(log(non_residential_sqft) ~ log(gross_sqft), bldnres)
# lmfit2 <- lm(log(non_residential_sqft) ~ log(gross_sqft), bldnres[sqft_dif > 0])
# slmfit <- summary(lmfit)
# here we just set nonres sqft to gross_sqft
imp <- !(bld$building_type_id %in% c(19, 12, 4, 11, 6, 10)) & !is.na(bld$gross_sqft) & is.na(bld$non_residential_sqft)
#lmpred <- predict(lmfit, bld[imp,]) + rnorm(sum(imp), 0, slmfit$sigma)
#bld[imp, non_residential_sqft := as.integer(round(exp(lmpred)))]
bld[imp, non_residential_sqft := gross_sqft]
imputed <- rep(FALSE, nrow(bld))
imputed[which(imp)] <- TRUE
bld <- bld[, imp_non_residential_sqft := imputed]
cat('\nImputed ', sum(imp), ' records of non_residential_sqft for non-residential buildings.')
# use the same fit to impute net_sqft for mixed use buildings
if(impute.net.sqft) {
	idx <- which(bld$building_type_id %in% c(10, 33) & !is.na(bld$gross_sqft) & is.na(bld$net_sqft))
	if(length(idx) > 0) {
		lmpred <- predict(lmfit, bld[idx,]) + rnorm(length(idx), slmfit$sigma)
		bld[idx,'net_sqft'] <- exp(lmpred)
		bld[idx, 'imp_net_sqft'] <- TRUE
	}
	cat('\nImputed ', length(idx), ' records of net_sqft for mixed-use buildings.')
	# for non-res buildings that have missing non_residential_sqft but not missing net_sqft, make them equal 
	# should this go at the beginning of this section? 132 records at the beginning vs 32 at the end
	idx <- which(with(bld, is.na(non_residential_sqft) & !is.na(net_sqft) & !(building_type_id %in% c(10, 33))))
	bld[idx,'net_sqft'] <- bld[idx,'non_residential_sqft']
	bld[idx, 'imp_net_sqft'] <- TRUE
	cat('\nImputed ', length(idx), ' records of net_sqft for non-res buildings where non_residential_sqft were not missing.')
}

# change building_type_id to 19 for buildings on single-family land use type and set # DU to 1
imp <- !is.na(bld$land_use_type_id) & bld$land_use_type_id==24 & is.na(bld$residential_units)
bld[imp, residential_units := 1]
bld[imp, imp_residential_units := TRUE]
bld[imp, building_type_id := 19]
cat('\nImputed ', sum(bld[imp, residential_units]), ' residential units for building on single-family land. building_type_id changed to 19.')

# recompute is.res
is.res <- bld$building_type_id %in% residential.bts


# impute residential units for remaining multi-family
# 1. records that have improvement value
imp <- bld$building_type_id %in% c(12, 4) & !is.na(bld$net_sqft) & bld$net_sqft > 0 & !is.na(bld$improvement_value) & bld$improvement_value > 0 & is.na(bld$residential_units)
ind <- with(bld, building_type_id %in% c(12, 4) & !is.na(net_sqft) & !is.na(residential_units) & residential_units > 0 & net_sqft > 1)
if(sum(imp) > 0) { 
	resest1 <- subset(bld, ind  & !is.na(improvement_value))
	lmfit1 <- lm(log(residential_units) ~ log(net_sqft) + log(improvement_value), resest1)
	slmfit1 <- summary(lmfit1)
	lmpred <- predict(lmfit1, bld[imp,])  + rnorm(sum(imp), 0, slmfit1$sigma)
	bld[imp, residential_units := as.integer(pmax(1, round(exp(lmpred))))]
	bld[imp, imp_residential_units := TRUE]
	cat('\nImputed ', sum(bld[imp, residential_units]), '(', sum(imp), ' records) residential units for multi-family buildings using net_sqft and improvement value.')
}

# 2. records that have missing improvement value
imp <- bld$building_type_id %in% c(12, 4) & !is.na(bld$net_sqft) & bld$net_sqft > 0 & is.na(bld$residential_units)
if(sum(imp) > 0) {
	resest2 <- subset(bld, ind)
	lmfit2 <- lm(log(residential_units) ~ log(net_sqft), resest2)
	slmfit2 <- summary(lmfit2)
	lmpred <- predict(lmfit2, bld[imp,])  + rnorm(sum(imp), 0, slmfit2$sigma)
	bld[imp, residential_units := pmax(1, round(exp(lmpred)))]
	bld[imp, imp_residential_units := TRUE]
	cat('\nImputed ', sum(bld[imp, residential_units]), '(', sum(imp), ' records) residential units for multi-family buildings using net_sqft only.')
}

# set group-quaters DUs to 0
imp <- with(bld,  !is.na(building_type_id) & building_type_id == 6 & residential_units > 0)
bld[imp, residential_units := 0]
bld[imp, imp_residential_units := TRUE]

# set the remainder of missing residential units to 1 
imp <- with(bld,  is.res & is.na(residential_units))
bld[imp, residential_units := 1]
bld[imp, imp_residential_units := TRUE]
cat('\nImputed ', sum(bld[imp, residential_units]), ' residential units for the remainder of residential records.')


library(mclust)
# # impute non-res sqft to remaining non-residential
# for remaining mix-use, split the gross_sqft
ind <- with(bld, is.nonres & is.na(non_residential_sqft) & !is.na(gross_sqft))
ind.du <- with(bld, ind & !is.na(residential_units) & !is.na(sqft_per_unit))
bld[ind.du, non_residential_sqft := pmax(0, gross_sqft - residential_units*sqft_per_unit)]
bld[ind & !ind.du, non_residential_sqft := gross_sqft]
bld[ind & !ind.du & is.na(residential_units), residential_units := 0]
bld[ind, imp_non_residential_sqft := TRUE]

nresest <- subset(bld, is.nonres & !is.na(non_residential_sqft) & !(building_type_id %in% c(10)) & !is.na(improvement_value) & !imp_non_residential_sqft & improvement_value > 1 & non_residential_sqft > 1)
# # remove upper cluster from King county data
nresest.King <- subset(nresest, county_id %in% c(33))
clust.data <- log(nresest.King[,c('non_residential_sqft', 'improvement_value')])
clust.sample.idx <- sample(1:nrow(clust.data), 5000)
# #icl <- mclustICL(clust.data[clust.sample.idx,])
# #summary(icl)
clustmod <- Mclust(clust.data, G=2, modelNames="EVV", initialization=list(subset=clust.sample.idx))
remove.bld.id <- nresest.King$building_id[clustmod$classification==1]
#plot(non_residential_sqft ~ improvement_value, data = nresest, log="xy")
#points(non_residential_sqft ~ improvement_value, data = nresest[building_id %in% remove.bld.id], col = "red")
nresest <- subset(nresest, !building_id %in% remove.bld.id)
lmfit <- lm(log(non_residential_sqft) ~ log(improvement_value), nresest)
impute <- with(bld,  is.nonres & is.na(non_residential_sqft) & !is.na(improvement_value) & !building_type_id %in% c(10) & !imp_non_residential_sqft)
lmpred <- predict(lmfit, bld[impute,]) + rnorm(sum(impute), 0, summary(lmfit)$sigma)
bld[impute, non_residential_sqft := as.integer(exp(lmpred))]
bld[impute, imp_non_residential_sqft := TRUE]

# impute buildings with missing improvement value but non-missing land value and parcel_sqft
bld[, Nbld := .N, by = parcel_id]
bld[, is_nr := is.nonres]
bld[, Nnonres := sum(is_nr), by = parcel_id]

estimate.nrsqft <- function(dat)
    lm(log(non_residential_sqft) ~ log(I(land_value/Nbld)) + log(I(parcel_sqft/Nbld)) + I(Nbld-Nother_nonres) + Nnonres, dat)
nresest <- subset(bld, is.nonres & !is.na(non_residential_sqft) & !(building_type_id %in% c(10)) & !is.na(land_value) & !imp_non_residential_sqft & non_residential_sqft > 10)
lmfit <- estimate.nrsqft(nresest)
# remove influential points using the Cook Distance
cookd <- cooks.distance(lmfit)
nresest <- subset(nresest, cookd < 0.5)
lmfit <- estimate.nrsqft(nresest)

impute <- with(bld,  is.nonres & is.na(non_residential_sqft) &  !building_type_id %in% c(10) & !imp_non_residential_sqft & !is.na(land_value))
lmpred <- predict(lmfit, bld[impute,]) + rnorm(sum(impute), 0, summary(lmfit)$sigma)
exppred <- as.integer(round(exp(lmpred)))
# distribute values smaller than 2 between 2 and 10
exppred[exppred < 2] <- sample(2:10, sum(exppred < 2), replace = TRUE)
bld[impute, non_residential_sqft := exppred]
bld[impute, imp_non_residential_sqft := TRUE]
bld[, `:=`(Nbld = NULL, is_nr = NULL, Nnonres = NULL)]

# Fill in gross_sqft where we imputed non-res sqft
imp <- with(bld, is.na(gross_sqft) & !is.na(non_residential_sqft) & imp_non_residential_sqft == TRUE)
bld[imp, gross_sqft := non_residential_sqft]
bld[imp, imp_gross_sqft := TRUE]






# library(mice)
# bldpat <- bld[,c('building_type_id', 'residential_units', 'non_residential_sqft', 'improvement_value', 'year_built', 'stories')]
# bldpat <- bld[,c('building_type_id', 'residential_units', 'non_residential_sqft', 'improvement_value', 'net_sqft')]
# bldpat <- bld[,c('building_type_id', 'residential_units', 'non_residential_sqft', 'improvement_value')]
# bldpat <- bld[,c('residential_units', 'non_residential_sqft', 'improvement_value', 'gross_sqft')]
# pat <- md.pattern(bldpat)
# rowvals <- as.integer(rownames(pat))
# o <- order(rowvals, decreasing=TRUE)
# pat[o,]

# convert logical to integer
logical.cols <- colnames(bld)[sapply(bld, is.logical)]
#for(attr in logical.cols) bld[[attr]] <- as.integer(bld[[attr]])
#for(attr in colnames(bld)) bld[is.na(bld[[attr]]), attr] <- 0
# write out resulting buildings	
#write.table(bld, file=file.path(data.dir, "imputed_buildings.csv"), sep=',', row.names=FALSE)
cat('\nResults written into imputed_consolidated_buildings.csv\n')


# The code below is for some diagnostics only
################################################## 
# compl <- pat[pat[,'residential_units']==1 & pat[,'non_residential_sqft']==1,]
# s <- sum(as.integer(rownames(compl)))
# s/nrow(bld)*100

# library(psych)
# pairs.panels(bldres[,c('net_sqft', 'gross_sqft', 'stories', 'improvement_value')])
# pairs.panels(mf[,c('net_sqft', 'log_sqft_per_unit', 'stories', 'improvement_value')])
# pairs.panels(resest[,c('residential_units', 'net_sqft',  'improvement_value', 'stories')])

# dt <- data.table(res)
# dt[,length(building_id), by=building_type_id][order(building_type_id)]
# dt[,length(building_id), by=land_use_type_id][order(land_use_type_id)]
# # look at duplicates in parcels for mobile homes
# mh <- data.table(subset(res, land_use_type_id==13))
# tmp <- mh[, .N, by=psrcpin]
# tmp[,length(psrcpin), by=N]

# library(VIM)
# marginplot(log(bld[!(bld$building_type_id %in% c(19, 12, 4, 11, 34, 10, 33)), c('non_residential_sqft', 'improvement_value')]))

# library(car)
# scatterplot(sqrt(net_sqft) ~ sqrt(gross_sqft), data=bldres1)
# scatterplot(non_residential_sqft ~ improvement_value, data=nresest)

# sbld <- subset(bld, non_residential_sqft > 0 & county_id == 33)
# nresest <- sbld
# bts <- sort(unique(nresest$building_type_id))
# pdf('nres_nonres_sqft_imprvalue_byBT_King.pdf', width=9)
# for(bt in bts) {
	# scatterplot(improvement_value ~ non_residential_sqft, data=subset(nresest, building_type_id==bt), log="xy", main=paste('building type', bt))
# }
 # dev.off()

# bts <- sort(unique(nresbld$building_type_id))
# pdf('nres_imprvalue_land_value_parcelsqft_byBT.pdf', width=9)
# for(bt in bts) {
	# d <- subset(nresbld, building_type_id==bt)
	# scatterplot(improvement_value ~ land_value, data=d, log="xy", main=paste('building type', bt))
	# scatterplot(improvement_value ~ parcel_sqft, data=d, log="xy", main=paste('building type', bt))
# }
# dev.off()

# summary(lm(log(improvement_value) ~ log(land_value) + log(parcel_sqft), data=nresbld))
# for(bt in bts) {
	# cat('\nBT ', bt, '\n')
	# d <- subset(nresbld, building_type_id==bt)
	# print(summary(lm(log(improvement_value) ~ log(land_value) + log(parcel_sqft), data=d)))
# }

	
# look at sqft_per_unit for MF-res
# bldmf <- subset(bld, building_type_id %in% c(12, 4))
# dtmf <- data.table(subset(bldmf, !is.na(residential_units) & residential_units > 0))
# dtmf[, list(avg_sqft_per_unit=mean(gross_sqft/residential_units, na.rm=TRUE), Nbuildings=.N), by=county_id]

# look at non-res buildings with residential units
# idx <- with(bld, !is.na(non_residential_sqft) & residential_units > 0 & !is.na(residential_units) & non_residential_sqft > 0 & !is.res  & !building_type_id %in% c(10,33,6))
# dt <- data.table(bld[idx,])
# dt[,sum(residential_units), by=.(land_use_type_id, building_type_id)]
# by cities
# cities <- read.table(file.path(data.dir, 'cities.csv'), sep=',', header=TRUE)
# bc <- dt[,list(DU=sum(residential_units), sqft=sum(non_residential_sqft)), by=city_id] %>% merge(cities[,c('city_id', 'city_name')], by='city_id')
# bc[order(bc$DU, decreasing=TRUE),]

# sum(subset(bld, !building_type_id %in% c(10,33,6, residential.bts) & residential_units > 0 & non_residential_sqft > 0)$residential_units, na.rm=TRUE)

# summary of imputed values
dt <- data.table(bld)
s <- dt[, list(non_residential_sqft=sum(non_residential_sqft, na.rm=TRUE)/43560, residential_units=sum(residential_units, na.rm=TRUE)), by=county_id]
dtr <- data.table(bld.raw)
sr <- dtr[, list(non_residential_sqft=sum(non_residential_sqft, na.rm=TRUE)/43560, residential_units=sum(residential_units, na.rm=TRUE)), by=county_id]
ms <- merge(sr, s, by='county_id')
ms <- cbind(ms, dif_non_res_sqft=ms$non_residential_sqft.y-ms$non_residential_sqft.x, dif_du=ms$residential_units.y-ms$residential_units.x)
ms[,list(sum(non_residential_sqft.x), sum(residential_units.x), sum(non_residential_sqft.y), sum(residential_units.y), sum(dif_non_res_sqft), sum(dif_du))]
