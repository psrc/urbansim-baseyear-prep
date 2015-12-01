# Hana Sevcikova, PSRC
# 11/3/2015
# The script matches residential units to Census counts.
# Inputs: 
#	data.year - which year all the data files correspond to
#	The following files are expected to be in the directory "data{data.year}"
#	file imputed_buildings.csv (output of impute_buildings.R)
#	file of HH totals - it is read via the file data{data.year}/read_hh_totals.R
#			where the name of the file is set,
#			e.g. households2.csv = output of synthpop, or an OFM dataset.
#	file parcels.csv (parcels dataset)
# Outputs:
#	All output files are written into "data{data.year}".
#	file imputed_buildings_matched.csv - It has adjusted residential_units.
#	file imputed_buildings_matched_for_opus.csv - as above but has some parcels attributes removed. 
#		The latter can be used to convert to an opus cache using:
#		python -m opus_core.tools.convert_table csv flt -o ~/opus_cache/2010 -d . -t imputed_buildings_matched_for_opus			

library(data.table)
library(magrittr)

data.year <- 2010 # data files will be taken from "data{data.year}"
data.dir <- paste0("data", data.year)
pcl.block.group.id.name <- "census_2010_block_group_id"

# load data
bld.raw <- read.table(file.path(data.dir, "imputed_buildings.csv"), sep=',', header=TRUE)
source(file.path(data.dir, "read_hh_totals.R")) # reads HH totals from the data directory (creates object hhtots)
pcl <- read.table(file.path(data.dir, "parcels.csv"), sep=',', header=TRUE)

# extract tract and block group attributes
pcl %<>% cbind(tract= (extract2(., pcl.block.group.id.name) %>% substr(6,11) %>% as.integer))
pcl %<>% cbind(block_group=(extract2(., pcl.block.group.id.name) %>% substr(12, 12) %>% as.integer)) %>% data.table
setkey(pcl, psrcpin)

# set-up the buildings table and merge with parcels
bld <- data.table(bld.raw)
setkey(bld, building_id, psrcpin)
bld %<>% cbind(residential_units_orig=extract2(., "residential_units"))
bld[bld$imp_residential_units>0,'residential_units_orig'] <- 0
bld %<>% merge(pcl[,c("psrcpin", pcl.block.group.id.name, "county_id", "tract", "block_group"), with=FALSE])

mftypes <- c(12, 4)
allrestypes <- c(12, 4, 19, 11, 34, 10, 33)
du <- bld[, sum(residential_units), by=c('county_id', 'tract', 'block_group')]
colnames(du)[4] <- 'DU'
nbld <- bld[, .N, by=c('county_id', 'tract', 'block_group')]
nmfbld <- bld[building_type_id %in% mftypes, .N, by=c('county_id', 'tract', 'block_group')]
colnames(nmfbld)[4] <- 'Nmf'
resbld <- bld[building_type_id %in% allrestypes, .N, by=c('county_id', 'tract', 'block_group')]
colnames(resbld)[4] <- 'Nres'
impmfbld <- bld[building_type_id %in% mftypes & imp_residential_units > 0, .N, by=c('county_id', 'tract', 'block_group')]
colnames(impmfbld)[4] <- 'Nimpmf'
nbld %<>% merge(nmfbld, by=c('county_id', 'tract', 'block_group'), all.x=TRUE)
nbld[is.na(nbld$Nmf), 'Nmf'] <- 0
nbld %<>% merge(resbld, by=c('county_id', 'tract', 'block_group'), all.x=TRUE)
nbld[is.na(nbld$Nres), 'Nres'] <- 0
nbld %<>% merge(impmfbld, by=c('county_id', 'tract', 'block_group'), all.x=TRUE)
nbld[is.na(nbld$Nimpmf), 'Nimpmf'] <- 0
du %<>% merge(nbld, by=c('county_id', 'tract', 'block_group'))

# merge with HH totals (created in read_hh_totals.R)
duhh <- merge(du, hhtots, by=c('county_id', 'tract', 'block_group'))
negdt <- subset(duhh, DU < HH)
print(sum(with(negdt, HH-DU)))

set.seed(1)
imputed.du <- 0
imputed.bld <- 0
# 1 building: place DU independent of type of building
s <- subset(negdt, N == 1)
for (i in 1:nrow(s)){
	bidx <- which(with(bld, county_id == s$county_id[i] & tract == s$tract[i] & block_group == s$block_group[i]))
	bld[bidx, "residential_units"] <- bld$residential_units[bidx] + s$HH[i] - s$DU[i]
	bld[bidx, "imp_residential_units"] <- 1
	imputed.du <- imputed.du + s$HH[i] - s$DU[i]
	imputed.bld <- imputed.bld + 1
}
cat('\nImputed ', imputed.du, ' units into ', imputed.bld, ' buildings for block groups with 1 building.')

# > 1 buildings & 1 multi-family building: place DU into that MF building
last.imputed.du <- imputed.du
last.imputed.bld <- imputed.bld
s <- subset(negdt, N > 1 & Nmf==1)
for (i in 1:nrow(s)){
	bidx <- which(with(bld, county_id == s$county_id[i] & tract == s$tract[i] & block_group == s$block_group[i] & building_type_id %in% mftypes))
	bld[bidx, "residential_units"] <- bld$residential_units[bidx] + s$HH[i] - s$DU[i]
	bld[bidx, "imp_residential_units"] <- 1
	imputed.du <- imputed.du + s$HH[i] - s$DU[i]
	imputed.bld <- imputed.bld + 1
}
cat('\nImputed ', imputed.du-last.imputed.du, ' units into ', imputed.bld-last.imputed.bld, ' buildings for block groups with 1 MF building.')

# > 1 multi-family buildings
last.imputed.du <- imputed.du
last.imputed.bld <- imputed.bld
imputed.du.to.imp <- 0
imputed.bld.to.imp <- 0
s <- subset(negdt, N > 1 & Nmf>1)
for (i in 1:nrow(s)){
	bidx <- which(with(bld, county_id == s$county_id[i] & tract == s$tract[i] & block_group == s$block_group[i] & building_type_id %in% mftypes))
	bidx.imp <- which(bld$imp_residential_units[bidx]>0)
	if(length(bidx.imp) > 0) { # add units to buildings where the DUs were imputed
		imp.idx <- bidx[bidx.imp]
		imputed.du.to.imp <- imputed.du.to.imp + s$HH[i] - s$DU[i]		
	} else { # distribute units across MF buildings
		imp.idx <- bidx
	}
	probs <- bld$residential_units[imp.idx]/sum(bld$residential_units[imp.idx])
	if(length(imp.idx)==1) {
		imp.idx <- rep(imp.idx,2) # sample interprets things differently if the first number is just one integer
		probs <- rep(probs,2)
	}
	sampled.idx <- sample(imp.idx, s$HH[i] - s$DU[i], replace=TRUE, prob=probs)
	tab <- table(sampled.idx)
	row.idx <- as.integer(names(tab))
	bld[row.idx, "residential_units"] <- bld$residential_units[row.idx] + tab
	bld[row.idx, "imp_residential_units"] <- 1
	imputed.du <- imputed.du + s$HH[i] - s$DU[i]
	imputed.bld <- imputed.bld + length(row.idx)
	if(length(bidx.imp) > 0) imputed.bld.to.imp <- imputed.bld.to.imp + length(row.idx)
}
cat('\nImputed ', imputed.du-last.imputed.du, ' units into ', imputed.bld-last.imputed.bld, ' buildings for ', nrow(s), ' block groups with multiple MF buildings, from which ', 
	imputed.du.to.imp, ' DUs were imputed into ', imputed.bld.to.imp, ' buildings that already had imputed DUs.')

# 0 multi-family buildings & > 0 residential (other residential type than MF)
last.imputed.du <- imputed.du
last.imputed.bld <- imputed.bld
imputed.du.to.imp <- 0
imputed.bld.to.imp <- 0
s <- subset(negdt, N > 1 & Nmf==0 & Nres > 0)
for (i in 1:nrow(s)){
	bidx <- which(with(bld, county_id == s$county_id[i] & tract == s$tract[i] & block_group == s$block_group[i] & building_type_id %in% allrestypes))
	bidx.imp <- which(bld$imp_residential_units[bidx]>0)
	if(length(bidx.imp) > 0) { # add units to buildings where the DUs were imputed
		imp.idx <- bidx[bidx.imp]
		imputed.du.to.imp <- imputed.du.to.imp + s$HH[i] - s$DU[i]		
	} else { # distribute units across res buildings
		imp.idx <- bidx
	}
	probs <- bld$residential_units[imp.idx]/sum(bld$residential_units[imp.idx])
	if(length(imp.idx)==1) {
		imp.idx <- rep(imp.idx,2) # sample interprets things differently if the first number is just one integer
		probs <- rep(probs,2)
	}
	sampled.idx <- sample(imp.idx, s$HH[i] - s$DU[i], replace=TRUE, prob=probs)
	tab <- table(sampled.idx)
	row.idx <- as.integer(names(tab))
	bld[row.idx, "residential_units"] <- bld$residential_units[row.idx] + tab
	bld[row.idx, "imp_residential_units"] <- 1
	imputed.du <- imputed.du + s$HH[i] - s$DU[i]
	imputed.bld <- imputed.bld + length(row.idx)
	if(length(bidx.imp) > 0) imputed.bld.to.imp <- imputed.bld.to.imp + length(row.idx)
}
cat('\nImputed ', imputed.du-last.imputed.du, ' units into ', imputed.bld-last.imputed.bld, ' buildings for ', nrow(s), 'block groups with non-MF residential buildings, from which ', 
	imputed.du.to.imp, ' DUs were imputed into ', imputed.bld.to.imp, ' buildings that already had imputed DUs.')

# 0 residentail buildings (only non-residential type)
last.imputed.du <- imputed.du
last.imputed.bld <- imputed.bld
imputed.du.to.imp <- 0
imputed.bld.to.imp <- 0
s <- subset(negdt, N > 1 & Nres == 0)
for (i in 1:nrow(s)){
	bidx <- which(with(bld, county_id == s$county_id[i] & tract == s$tract[i] & block_group == s$block_group[i]))
	bidx.imp <- which(bld$building_type_id[bidx] != bld$building_type_id_orig[bidx])
	if(length(bidx.imp) > 0) { # add units to buildings where building_type was imputed
		imp.idx <- bidx[bidx.imp]
		imputed.du.to.imp <- imputed.du.to.imp + s$HH[i] - s$DU[i]		
	} else { # distribute units across buildings
		imp.idx <- bidx
	}
	probs <- bld$non_residential_sqft[imp.idx]/sum(bld$non_residential_sqft[imp.idx]) # take non-res sqft as  a proxy for the size
	if(length(imp.idx)==1) {
		imp.idx <- rep(imp.idx,2) # sample interprets things differently if the first number is just one integer
		probs <- rep(probs,2)
	}
	sampled.idx <- sample(imp.idx, s$HH[i] - s$DU[i], replace=TRUE, prob=probs)
	tab <- table(sampled.idx)
	row.idx <- as.integer(names(tab))
	bld[row.idx, "residential_units"] <- bld$residential_units[row.idx] + tab
	bld[row.idx, "imp_residential_units"] <- 1
	imputed.du <- imputed.du + s$HH[i] - s$DU[i]
	imputed.bld <- imputed.bld + length(row.idx)
	if(length(bidx.imp) > 0) {
		imputed.bld.to.imp <- imputed.bld.to.imp + length(row.idx)
		bld[row.idx, "building_type_id"] <- 12 # set to MF residential
	}
}
cat('\nImputed ', imputed.du-last.imputed.du, ' units into ', imputed.bld-last.imputed.bld, ' buildings for ', nrow(s), 'block groups with non-residential buildings, from which ', 
	imputed.du.to.imp, ' DUs were imputed into ', imputed.bld.to.imp, ' buildings imputed building_type_id.')
cat('\nTotals: ', imputed.du, 'units, ', imputed.bld, ' buildings')

# Reduce DUs for MF-buildings where DUs were imputed and Census numbers are lower
posdt <- subset(duhh, DU > 1.1*HH & Nimpmf > 0 & Nmf > 0)
s <- posdt
reduced.du <- 0
reduced.bld <- 0
#bld2 <- bld
for (i in 1:nrow(s)){
	bidx <- which(with(bld, county_id == s$county_id[i] & tract == s$tract[i] & block_group == s$block_group[i] & building_type_id %in% mftypes & imp_residential_units > 0))
	reduction <- s$DU[i] - s$HH[i]
	probs <- bld$residential_units[bidx]/sum(bld$residential_units[bidx])
	if(length(bidx)==1) {
		bidx <- rep(bidx,2) # sample interprets things differently if the first number is just one integer
		probs <- rep(probs,2)
	}
	sampled.idx <- sample(bidx, reduction, replace=TRUE, prob=probs)
	tab <- table(sampled.idx)
	row.idx <- as.integer(names(tab))
	value <- pmax(max(bld$stories[row.idx],1)*2, bld$residential_units[row.idx] - tab)
	reduced.du <- reduced.du + sum(bld$residential_units[row.idx] - value)
	reduced.bld <- reduced.bld + sum((bld$residential_units[row.idx] - value) > 0)
	bld[row.idx, "residential_units"] <- as.integer(value)
}
cat('\nResidential units reduced by ', reduced.du, ' in ', reduced.bld, ' buildings from ', nrow(s), 'block groups.') 
tot <- sum(with(bld, residential_units))
tot.orig <- sum(with(bld, residential_units_orig))
cat('\nTotal change: ', tot-tot.orig, ' residential units (from ', tot.orig, ' to ', tot, ')')

# write outputs
write.table(bld, file=file.path(data.dir, "imputed_buildings_matched.csv"), sep=',', row.names=FALSE)
# for exportng to opus cache, remove the parcels attributes, since they are not needed
bld.for.opus <- bld[,-which(colnames(bld)%in% c('census_2010_block_group_id', 'tract', 'block_group', 'is_inside_urban_growth_boundary', 'tax_exempt')), with=FALSE]
colnames(bld.for.opus)[1] <- 'parcel_id'
write.table(bld.for.opus, file=file.path(data.dir, "imputed_buildings_matched_for_opus.csv"), sep=',', row.names=FALSE)


# The code below is for some diagnostics only
##################################################
## get parcels with latitude and longitude
# pcllatlon.all <- data.table(read.table("~/workspace/data/psrc_parcel/gis/2010/parcels2010latlon.csv", sep=',', header=TRUE))
# setkey(pcllatlon.all, psrcpin)
# pcllatlon <- merge(pcllatlon.all[psrcpin > 0,.(psrcpin, latlon)], pcl[,.(psrcpin, county_id, tract, block_group)])
# pcllatlonB <- pcllatlon[psrcpin %in% bld$psrcpin]

# library(googleVis)
# s <- subset(negdt, HH-DU > 650)
# p <- subset(pcllatlonB, county_id %in% s$county_id & tract %in% s$tract & block_group %in% s$block_group)
# data.to.plot <- merge(p, cbind(s, tipvar=paste('DU:', s$DU, '<br>HH:', s$HH)), by=c('county_id', 'tract', 'block_group'))
# map <- gvisMap(data.to.plot, "latlon", tipvar='tipvar', options=list(showTip=TRUE, enableScrollWheel=TRUE, height="40cm"))
# plot(map)

# png("hist_diff_hh_du.png", width=800, height=400)
# par(mfrow=c(1,2))
# hist(with(duhh, DU-HH), breaks=20, main='DU - HH by block groups', xlab='DU - HH')
# legend('topright', legend=c(paste("N =", nrow(duhh))))
# hist(with(negdt, DU-HH), breaks=20, main='DU - HH by block groups (overflow only)', xlab='DU - HH')
# legend('topleft', legend=c(paste("N =", nrow(negdt))))
# dev.off()

# png("hist_diff_hh_du_pos.png", width=600, height=400)
# hist(with(posdt, DU-HH), breaks=20, main='DU - HH by block groups where DU > 1.2*HH & imputed DUs', xlab='DU - HH')
# legend('topright', legend=c(paste("N =", nrow(posdt))))
# dev.off()

# # show corners of the area
# lon <- range(subset(pcllatlon.all, x_coord_sp>0)$lon)
# lat <- range(subset(pcllatlon.all, y_coord_sp>0)$lat)
# lon <- rep(lon, 2)
# lat <- rep(lat, each=2)
# df <- data.frame(latlon=paste(lat,lon,sep=':'), const=rep(1, 4))
# map <- gvisMap(df, "latlon", options=list(enableScrollWheel=TRUE, height="40cm"))
# plot(map)