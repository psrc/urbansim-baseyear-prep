# Hana Sevcikova, PSRC
# 11/3/2015
# The script imputes building_type_id, residential_units and non_residential_sqft (partly also improvement_value) using a set of regression models.
# All I/O files are in the directory "data{data.year}" where data.year should be set to correspond to the datasets.
# Input: file buildings_raw.csv
# 		In mysql create a table buildings_raw which is a selection of specific buildings attributes joint with some parcels attributes. Use this query:
# 		create table buildings_hana select a.psrcpin, building_id, year_built, gross_sqft, net_sqft, non_residential_sqft, residential_units, sqft_per_unit, stories, building_type_id, a.improvement_value, county, land_use_type_id, parcel_sqft, tax_exempt, land_value, is_inside_urban_growth_boundary from buildings_estimated as a left join parcels_estimated as b on a.psrcpin=b.psrcpin;
# 		Then export the result into a csv file called buildings_raw.csv.
# Output: file imputed_buildings.csv
#		It has the missing values filled in as well as new columns indicating which records were imputed.


library(data.table)
data.year <- 2014 # data files will be taken from "data{data.year}"
data.dir <- paste0("data", data.year)
#bld.raw <- read.table(file.path(data.dir, 'buildings_raw.csv'), sep=',', header=TRUE)
bld.raw <- read.table(file.path(data.dir, 'buildings_raw.tab'), sep='\t', header=TRUE, stringsAsFactors=FALSE, quote="\`")
pcl_id <- if(data.year < 2014) 'psrcpin' else 'parcel_id'
impute.net.sqft <- data.year < 2014
residential.bts <- c(19, 12, 4, 11, 34)

# impute building_type_id from land use types
lut.bt.pairs <- list('1'=1, # agriculture 
					 '2'=2, # civic
					 '3'=3, # commercial
					 '4'=24, # fisheries
					 '5'=25, # forest harvestable
					 '6'=26, # forest protected
					 '7'=5, # government
					 '8'=6, # group quaters
					 '9'=7, # hospital
					 '10'=8, # industrial
					 '11'=9, # military
					 '12'=27, # mining
					 '13'=34, # mobile home park
					 '14'=12, # multi-family
					 '15'= 4, # condo
					 '17'=22, # no code
					 '18'=13, # office
					 '19'=15, # park 
					 '20'=16, # parking
					 '21'=21, # recreation
					 '22'=30, # right of way
					 '23'=18, # school
					 '24'=19, # single-family
					 '25'=20, # tcu
					 '26'=32, # vacant
					 '28'=21, # warehousing
					 '29'=15, # water into open space
					 '30'=10 # mixed use					 
					 )
bld <- bld.raw
set.seed(1)

# impute/change building type for missing BT or for "no code" and "vacant" BT
orig_builidng_type <- bld$building_type_id
bld <- cbind(bld, building_type_id_orig=orig_builidng_type)
nounits <- bld$residential_units == 0 & bld$non_residential_sqft == 0
for(lut in names(lut.bt.pairs)) {
	lut.idx <- bld$land_use_type_id==as.integer(lut)
	nocode.vacant <- bld$building_type_id %in% c(28, 32, 22)
	idx <- lut.idx & ( bld$building_type_id==0 | (nocode.vacant & nounits))
	if(lut %in% c(13:15,24)) {# if residential LUT and ...
		# ... non-res-sqft is zero then convert "no code" and "vacant" BT 
		idx <- idx | (lut.idx & nocode.vacant & bld$non_residential_sqft == 0)
		# ... non-res BT (excl. mixed use and group quarters) and residential units are non-zero and non_residential_sqft is zero, then change BT
		idx <- idx | (lut.idx & !bld$building_type_id %in% c(10,33,6, residential.bts) & bld$residential_units > 0 & bld$non_residential_sqft == 0)
	}
	bld[which(idx), 'building_type_id'] <- lut.bt.pairs[[lut]]
}
# reclassify records for LUT 30 that have res units and no non-res sqft into MF 
idx <- with(bld, land_use_type_id==30 & !building_type_id %in% c(10,33,6, residential.bts) & residential_units > 0 & non_residential_sqft == 0)
bld[which(idx), 'building_type_id'] <- 12
cat('\nImputed ', sum(bld$building_type_id != bld$building_type_id_orig), ' values of building_type_id.')

#impute single family
before.du <- sum(bld$residential_units)
imputed <- rep(FALSE, nrow(bld))
idx <- bld$building_type_id %in% c(19, 11) & bld$residential_units == 0
# special cases of SF
#special.parcels <- c(492180) 
special.parcels <- c()
idx <- which(idx | (bld$parcel_id %in% special.parcels & bld$residential_units == 0 & bld$building_type_id %in% c(12,4)))
bld[idx,'residential_units'] <- 1
imputed[idx] <- TRUE
bld <- cbind(bld, imp_residential_units=imputed)
cat('\nImputed ', sum(bld$residential_units)- before.du, ' single-family residential units.')


for(attr in c('building_type_id', 'gross_sqft', 'improvement_value', 'land_use_type_id', 'net_sqft', 'parcel_sqft', 'stories', 'year_built', 'land_value', 'sqft_per_unit')) {
	if(attr %in% colnames(bld))
		bld[bld[,attr]==0,attr] <- NA
}
is.res <- bld$building_type_id %in% residential.bts
nounits <- bld$residential_units == 0 & bld$non_residential_sqft == 0
bld[nounits &  is.res,'residential_units'] <- NA
bld[nounits &  !is.res,'non_residential_sqft'] <- NA
bld[nounits &  !is.na(bld$building_type_id) & bld$building_type_id %in% c(10, 33), 'residential_units'] <- NA # mixed use
bld[nounits &  is.na(bld$building_type_id), c('residential_units', 'non_residential_sqft')] <- NA
# special cases:
# King County: code 99 for missing values of stories
bld[!is.na(bld$stories) & bld$stories == 99 & bld$county == 33, 'stories'] <- NA
# King County: code 1000 for missing values of improvement value
bld[!is.na(bld$improvement_value) & bld$improvement_value == 1000 & bld$county == 33, 'improvement_value'] <- NA
# consider non_residential_sqft (and possibly net_sqft and gross_sqft) that equal to 1 as missing
bld[!is.na(bld$non_residential_sqft) & !is.res & bld$non_residential_sqft == 1, 'non_residential_sqft'] <- NA
bld[!is.na(bld$gross_sqft) & bld$gross_sqft == 1, 'gross_sqft'] <- NA
# set missing sqft_per_unit for MF residential records to 1000
bld[bld$building_type_id %in% c(12, 4) & is.na(bld$sqft_per_unit), 'sqft_per_unit'] <- 1000

# pdf('nonres_sqft_vs_imprvalue.pdf')
# plot(improvement_value ~ non_residential_sqft, subset(bld, improvement_value>0 & non_residential_sqft > 0 & !is.res), log="xy")
# dev.off()

# add column number_of_buildings
dt <- data.table(bld)
tmp <- dt[, list(number_of_buildings=.N, number_of_mf_buildings=sum(building_type_id %in% c(12,14))), by=pcl_id]
bld <- merge(bld, tmp, by=pcl_id)
is.res <- bld$building_type_id %in% residential.bts

# impute net_sqft for multi-family residential using linear regression 
if(impute.net.sqft) {
	ind <- with(bld, building_type_id %in% c(12, 4) & !is.na(net_sqft) & !is.na(gross_sqft) & net_sqft <= gross_sqft)
	bldres1 <- subset(bld, ind & !is.na(stories))
	bldres2 <- subset(bld, ind)
	# 1. records with non-missing stories
	lmfit1 <- lm(sqrt(net_sqft) ~ sqrt(gross_sqft) + stories, bldres1)
	idx <- which(bld$building_type_id %in% c(12, 4) & is.na(bld$net_sqft) & !is.na(bld$gross_sqft) & !is.na(bld$stories) & is.na(bld$residential_units))
	lmpred <- predict(lmfit1, bld[idx,])
	bld[idx,'net_sqft'] <- lmpred^2 # back-transform
	imputed <- rep(FALSE, nrow(bld))
	imputed[idx] <- TRUE
	bld <- cbind(bld, imp_net_sqft=imputed)
	cat('\nImputed ', length(idx), ' records of net_sqft for multi-family residential buildings using gross_sqft and stories.')
	# 2. records with missing stories
	lmfit2 <- lm(sqrt(net_sqft) ~ sqrt(gross_sqft), bldres2)
	idx <- which(bld$building_type_id %in% c(12, 4) & is.na(bld$net_sqft) & !is.na(bld$gross_sqft) & is.na(bld$residential_units))
	lmpred <- predict(lmfit2, bld[idx,])
	bld[idx,'net_sqft'] <- lmpred^2 # back-transform
	bld[idx, 'imp_net_sqft'] <- TRUE
	cat('\nImputed ', length(idx), ' records of net_sqft for multi-family residential buildings using gross_sqft.')
} else { # set net_sqft to gross_sqft
	bld <- cbind(bld, net_sqft=bld$gross_sqft)
}

# impute residential units if net_sqft and sqft_per_unit is not missing
idx <- which(is.res & is.na(bld$residential_units) & ! is.na(bld$net_sqft) & bld$sqft_per_unit > 0)
bld[idx, 'residential_units'] <- round(bld$net_sqft[idx]/bld$sqft_per_unit[idx])
bld[idx, 'imp_residential_units'] <- TRUE
cat('\nImputed ', sum(bld$residential_units[idx]), '(', length(idx), ' records) multi-family residential units as net_sqft/sqft_per_unit.')

# impute non_residential_sqft for non-residential using linear regression
bldnres <- subset(bld, !(building_type_id %in% c(19, 12, 4, 11, 34, 10, 33)) & !is.na(non_residential_sqft) & !is.na(gross_sqft) & non_residential_sqft > 0)
lmfit <- lm(log(non_residential_sqft) ~ log(gross_sqft), bldnres)
idx <- which(!(bld$building_type_id %in% c(19, 12, 4, 11, 34, 10, 33)) & !is.na(bld$gross_sqft) & is.na(bld$non_residential_sqft))
lmpred <- predict(lmfit, bld[idx,])
bld[idx,'non_residential_sqft'] <- exp(lmpred)
imputed <- rep(FALSE, nrow(bld))
imputed[idx] <- TRUE
bld <- cbind(bld, imp_non_residential_sqft=imputed)
cat('\nImputed ', length(idx), ' records of non_residential_sqft for non-residential buildings.')
# use the same fit to impute net_sqft for mixed use buildings
if(impute.net.sqft) {
	idx <- which(bld$building_type_id %in% c(10, 33) & !is.na(bld$gross_sqft) & is.na(bld$net_sqft))
	if(length(idx) > 0) {
		lmpred <- predict(lmfit, bld[idx,])
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

# change building_type_id to 11 for mobile homes, i.e. where land use type is 13 and set # DU to 1 for all mobile homes
idx <- which(bld$building_type_id %in% c(34, 13) & is.na(bld$residential_units))
bld[idx, 'residential_units'] <- 1
bld[idx, 'imp_residential_units'] <- TRUE
cat('\nImputed ', sum(bld[idx, 'residential_units']), ' residential units for mobile homes.')
idx.lu <- which(!(bld$building_type_id[idx] %in% c(11,34)))
bld[idx[idx.lu], 'building_type_id'] <- 11
cat('\n', length(idx.lu), ' values of building_type_id changed to mobile home.')

# change building_type_id to 19 for buildings on single-family land use type and set # DU to 1
idx <- which(bld$land_use_type_id==24 & is.na(bld$residential_units))
bld[idx, 'residential_units'] <- 1
bld[idx, 'imp_residential_units'] <- TRUE
bld[idx, 'building_type_id'] <- 19
cat('\nImputed ', sum(bld[idx, 'residential_units']), ' residential units for building on single-family land. building_type_id changed to 19.')

# recompute is.res
is.res <- bld$building_type_id %in% residential.bts

# impute residential units for remaining multi-family
# 1. records that have improvement value
idx <- which(bld$building_type_id %in% c(12, 4) & !is.na(bld$net_sqft) & bld$net_sqft > 0 & !is.na(bld$improvement_value) & bld$improvement_value > 0 & is.na(bld$residential_units))
ind <- with(bld, building_type_id %in% c(12, 4) & !is.na(net_sqft) & !is.na(residential_units) & residential_units > 0 & net_sqft > 1)
if(length(idx) > 0) {	
	resest1 <- subset(bld, ind  & !is.na(improvement_value))
	lmfit1 <- lm(log(residential_units) ~ log(net_sqft) + log(improvement_value), resest1)
	lmpred <- predict(lmfit1, bld[idx,])
	bld[idx, 'residential_units'] <- pmax(1, round(exp(lmpred)))
	bld[idx, 'imp_residential_units'] <- TRUE
	cat('\nImputed ', sum(bld[idx, 'residential_units']), '(', length(idx), ' records) residential units for multi-family buildings using net_sqft and improvement value.')
}

# 2. records that have missing improvement value
idx <- which(bld$building_type_id %in% c(12, 4) & !is.na(bld$net_sqft) & bld$net_sqft > 0 & is.na(bld$residential_units))
if(length(idx) > 0) {
	resest2 <- subset(bld, ind)
	lmfit2 <- lm(log(residential_units) ~ log(net_sqft), resest2)
	lmpred <- predict(lmfit2, bld[idx,])
	bld[idx, 'residential_units'] <- pmax(round(exp(lmpred)))
	bld[idx, 'imp_residential_units'] <- TRUE
	cat('\nImputed ', sum(bld[idx, 'residential_units']), '(', length(idx), ' records) residential units for multi-family buildings using net_sqft only.')
}

# impute non-res sqft to remaining non-residential
nresest <- subset(bld, !is.res & !is.na(non_residential_sqft) & !(building_type_id %in% c(10,33)) & !is.na(improvement_value) & !imp_non_residential_sqft & improvement_value > 1 & non_residential_sqft > 1)
lmfit.imprv <- lm(log(non_residential_sqft) ~ log(improvement_value), nresest)
idx <- which(with(bld,  !is.res & is.na(non_residential_sqft) & !(building_type_id %in% c(10,33)) & !is.na(improvement_value)))
lmpred <- predict(lmfit.imprv, bld[idx,])
bld[idx,'non_residential_sqft'] <- exp(lmpred)
bld[idx, 'imp_non_residential_sqft'] <- TRUE
cat('\nImputed ', length(idx), ' records of non_residential_sqft for non-residential buildings where improvement value is not missing.')

#library(mice)
#nresbld <- subset(bld, !is.res)
#nresbld <- nresbld[,c('building_type_id', 'non_residential_sqft', 'improvement_value', 'net_sqft', 'parcel_sqft', 'stories', 'year_built')]
#mice.nres <- mice(nresbld, maxit=3, m=1, seed=1)

# impute improvement value for non-res buildings 
nresbld <- subset(bld, !is.res & !is.na(land_value) & !is.na(improvement_value) &  !(building_type_id %in% c(10,33)))
lmfit <- lm(log(improvement_value) ~ log(land_value) +  log(number_of_buildings) + (is_inside_urban_growth_boundary > 0), data=nresbld)
idx <- which(with(bld,  !is.res & !is.na(land_value) & is.na(non_residential_sqft)  & !(building_type_id %in% c(10,33))))
lmpred <- predict(lmfit, bld[idx,])
bld[idx,'improvement_value'] <- exp(lmpred)
imputed <- rep(FALSE, nrow(bld))
imputed[idx] <- TRUE
bld <- cbind(bld, imp_improvement_value=imputed)
cat('\nImputed ', length(idx), ' records of improvement value for non-residential buildings.')
lmpred <- predict(lmfit.imprv, bld[idx,])
bld[idx,'non_residential_sqft'] <- exp(lmpred)
bld[idx, 'imp_non_residential_sqft'] <- TRUE
cat('\nImputed ', length(idx), ' records of non_residential_sqft for non-residential buildings where improvement value was imputed.')


library(mice)
bldpat <- bld[,c('building_type_id', 'residential_units', 'non_residential_sqft', 'improvement_value', 'year_built', 'stories')]
bldpat <- bld[,c('building_type_id', 'residential_units', 'non_residential_sqft', 'improvement_value', 'net_sqft')]
bldpat <- bld[,c('building_type_id', 'residential_units', 'non_residential_sqft', 'improvement_value')]
bldpat <- bld[,c('residential_units', 'non_residential_sqft', 'improvement_value', 'gross_sqft')]
pat <- md.pattern(bldpat)
rowvals <- as.integer(rownames(pat))
o <- order(rowvals, decreasing=TRUE)
pat[o,]

# convert logical to integer
logical.cols <- colnames(bld)[sapply(bld, is.logical)]
for(attr in logical.cols) bld[,attr] <- as.integer(bld[,attr])
for(attr in colnames(bld)) bld[which(is.na(bld[,attr])), attr] <- 0
# write out resulting buildings	
write.table(bld, file=file.path(data.dir, "imputed_buildings.csv"), sep=',', row.names=FALSE)
cat('\nResults written into imputed_buildings.csv\n")


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

# bts <- sort(unique(nresest$building_type_id))
# pdf('nres_nonres_sqft_imprvalue_byBT.pdf', width=9)
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
