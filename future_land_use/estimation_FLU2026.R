# Script for estimating coefficients to impute DU/acre and heights
# Hana Sevcikova (PSRC)
# 04/22/2026


library(data.table)
library(readxl)

# paths and file names
in.path <- "~/psrc/urbansim-baseyear-prep/future_land_use"
data.path <- file.path(in.path, "data2026")

# read the FLU file
#fluall <- fread(file.path(data.path, "Zoning_2026_d2.csv"))
fluall <- data.table(read_xlsx(file.path(data.path, "Zoning_2026_d2.xlsx")))
flubonus <- fluall[Bonus_included == "Y"]
flunobonus <- fluall[is.na(Bonus_included) | Bonus_included == ""]
dim(fluall)
dim(flubonus)
dim(flunobonus)
dim(flunobonus[!juris_zn %in% flubonus$juris_zn])
dim(flu)

# put together bonus zones and those that don't have bonuses
flu <- rbind(flubonus, flunobonus[!juris_zn %in% flubonus$juris_zn])

# remove error records
flu <- flu[Zone != "ERROR"]

# check for duplicates
flu[, .N, by = "juris_zn"][order(-N)]

# which columns should be considered to identify duplicates
cols.for.dupl <- setdiff(colnames(flu), c("Zone", "Definition", "Bonus_avail", "Bonus_included", "ADU notes", 
                                          colnames(flu)[startsWith(colnames(flu), "Unnamed")], "V48", "V49", "V50"))
uflu <- flu[!duplicated(flu, by = cols.for.dupl)] # removes true duplicates
# still contains duplicate juris_zn, but rows differ in other columns
paste(uflu[, .N, by = "juris_zn"][order(-N)][N > 1][, juris_zn], collapse = ", ")

# removed duplicates that are the same in all values except the column Zone
#uflu2 <- flu[!duplicated(flu, by = c("Zone", cols.for.dupl))]
#fluall[juris_zn %in% uflu2[, .N, by = "juris_zn"][N > 1, juris_zn] & !juris_zn %in% uflu[, .N, by = "juris_zn"][N > 1, juris_zn], .(Zone, juris_zn)][order(juris_zn)]

flu <- copy(uflu)

# clean various numeric columns
num.cols <- colnames(flu)[grepl("^Min|^Max|^ResDU|^LC", colnames(flu))]
for(col in num.cols){
    if(is.character(flu[[col]])){
        flu[get(col) %in% c("", "None"), (col) := NA]
        flu[get(col) == "unlimited", (col) := -1]   
    }
}

# fix Ht (some records have info in terms of stories, e.g. "3 story")
for(col in c("MaxHt_Res", "MaxHt_Comm", "MaxHt_Office", "MaxHt_Mixed"))
    flu[grepl(" story", get(col)), (col) := as.integer(gsub(" story", "", get(col))) * 12]

# there are some values in parentheses, remove it (but double check if it makes sense)
flu[,.N, by = "MaxFAR_Mixed"]
for(col in num.cols){
    flu[, (col) := gsub("\\s*\\([^\\)]+\\)", "", get(col))]
}
# In Sumner MaxDU_Res there are values defined as ranges, take the middle
col <- "MaxDU_Res"
flu[, c("rng1", "rng2") := tstrsplit(get(col), "-", type.convert = TRUE)]
flu[!is.na(rng2), c(col, "juris_zn", "rng1", "rng2"), with = FALSE] # view affected rows
flu[!is.na(rng2), (col) := round(rng1 + (rng2 - rng1)/2)][, `:=`(rng1 = NULL, rng2 = NULL)]

# convert numeric columns to numeric
# if there is a warning, investigate in which column and why, 
# by setting options(warn = 2) & flu[,.N, by = col]
for(col in num.cols)
    flu[, (col) := as.numeric(get(col))]

# clean the "rural" column
flu[, .N, by = "rural"]
flu[, rural := ifelse(rural == "Y", TRUE, ifelse(rural %in% c("N", "not"), FALSE, NA))]
# set various zones as "rural" (20 records)
# found via flu[grepl("rural", Definition) & is.na(rural)]
flu[(Zone %in% c("FL", "EPF-RAN", "RIC", "RNC", "RSR", "RR", "RW", "RCO", "SVLR", "FRL", "ARL", "RSep") |
          juris_zn %in% c("Pierce_County_GC", "Kenmore_GC", "Kenmore_P", "Kent_A-10", "Kitsap_RP",
                          "Kitsap_RI", "Kitsap_REC", "Kitsap_TTEC")) 
    & is.na(rural), rural := TRUE]

# set NA rural records to FALSE 
flu[is.na(rural), rural := FALSE]

# set to residential use if ResDU_lot given (four records found)
flu[((!is.na(ResDU_lot) & ResDU_lot > 0) | (!is.na(MaxDU_Res) & MaxDU_Res > 0)) & Res_Use != "Y", Res_Use := "Y"]

# if "missing middle" is in the description but neither ResDU_lot nor MaxDU_Res given,
# set ResDU_lot to -1 (i.e. follow the HC1110 law). It applies to two zones in Marysville
flu[grepl("missing middle", Definition) & Res_Use == "Y" & is.na(ResDU_lot) & is.na(MaxDU_Res),
    ResDU_lot := -1]

# if use-specific LC not given but LC_Mixed given, set the use-specific LC to that
for(use in c("Res", "Comm", "Office", "Indust")){
    flu[get(paste0(use, "_Use")) == "Y" & is.na(get(paste0("LC_", use))) & !is.na(LC_Mixed),
        (paste0("LC_", use)) := LC_Mixed]
}

# check which records are only Mixed use and nothing else
unique(flu[Mixed_Use == "Y" & Res_Use != "Y" & Comm_Use != "Y" & Indust_Use != "Y" & Office_Use != "Y" & grepl("residential", Definition), Juris])

# subset of residential records
rflu <- subset(flu, Res_Use == "Y")[, `ADU notes` := NULL]

# adjust height
rflu[, MaxHt_Res_orig := MaxHt_Res]

# additive FAR in Tacoma, Auburn, Kent
adj1 <- with (rflu, !is.na(MaxFAR_Res) & !is.na(MaxFAR_Mixed) & MaxFAR_Res < MaxFAR_Mixed & MaxFAR_Res > 0)
adj2 <- with (rflu, !is.na(MaxFAR_Res) & !is.na(MaxFAR_Mixed) & MaxFAR_Res > MaxFAR_Mixed & MaxFAR_Mixed > 0) # one record
rflu[adj1, .(juris_zn, #MinFAR_Res, MinFAR_Mixed, 
             MaxFAR_Res, MaxFAR_Mixed, MaxHt_Res, MaxHt_Mixed, MaxHt_Res_orig)]
rflu[adj1, MaxHt_Res := round(MaxHt_Res * MaxFAR_Res/MaxFAR_Mixed)]
rflu[adj2, MaxHt_Res := round(MaxHt_Res * MaxFAR_Res/(MaxFAR_Mixed + MaxFAR_Res))]

# for residential records without MaxDU_Res, MaxFAR_Res, ResDU_lot but with MaxDU_Mixed, use that for MaxDU_Res
rflu[is.na(MaxDU_Res) & is.na(MaxFAR_Res) & is.na(ResDU_lot) & !is.na(MaxDU_Mixed), MaxDU_Res := MaxDU_Mixed]

# for residential records without MaxHt_Res but with MaxHt_Mixed, use that for MaxHt_Res
rflu[is.na(MaxHt_Res) & !is.na(MaxHt_Mixed), MaxHt_Res := MaxHt_Mixed]

# for mobile home parks with missing DU/acre, impute 5
rflu[Zone == "MHP" & is.na(MaxDU_Res) & is.na(ResDU_lot), MaxDU_Res := 5]

# subset of residential records that will get DU/acre imputed
rflu.pred <- subset(rflu, is.na(MaxDU_Res) & is.na(ResDU_lot))

# summarize those records
rflu.pred[, .N, by = .(hasLC = !is.na(LC_Res), hasHt = !is.na(MaxHt_Res), hasFAR = !is.na(MaxFAR_Res) #, hasFARmx = !is.na(MaxFAR_Mixed)
                        )][order(hasLC, hasHt, hasFAR, decreasing = TRUE)][]
rflu.pred[is.na(LC_Res) & is.na(MaxHt_Res) & is.na(MaxFAR_Res)]

# records with no information
no.info.rows <- rflu.pred[is.na(LC_Res) & is.na(MaxHt_Res) & is.na(MaxFAR_Res)]
#paste(no.info.rows[, juris_zn], collapse = ", ")


# subset of residential records to be used in the estimation, i.e. DU/acre is not missing
rflu.est <- subset(rflu, !is.na(MaxDU_Res) & MaxDU_Res > 0)
# summarize those records
rflu.est[, .N, by = .(hasLC = !is.na(LC_Res), hasHt = !is.na(MaxHt_Res), hasFAR = !is.na(MaxFAR_Res))][order(hasLC, hasHt, hasFAR, decreasing = TRUE)][]
rflu.est[, .N, by = .(hasLC = !is.na(LC_Res), hasHt = !is.na(MaxHt_Res), rural = rural)][order(hasLC, hasHt, decreasing = TRUE)][]

# the estimation set needs to be reduced to those records that have non-missing height
sflu <- subset(rflu.est, !is.na(MaxHt_Res) & MaxHt_Res > 0)

# records to be imputed grouped depending if they have lot coverage missing and the imputing attribute:
# to be used for imputation of DU/acre from heights
sflu1.pred <- subset(rflu, !is.na(MaxHt_Res) & MaxHt_Res > 0 & !is.na(LC_Res) & is.na(MaxDU_Res))
sflu.pred <- subset(rflu, !is.na(MaxHt_Res) & MaxHt_Res > 0 & is.na(LC_Res) & is.na(MaxDU_Res))
# have nothing to support the imputation
rflu.pred.rest <- rflu.pred[!juris_zn %in% c(sflu1.pred$juris_zn, sflu.pred$juris_zn)]

# to be used for imputation of heights from DU/acre
sfluI1.pred <- subset(rflu, !is.na(MaxDU_Res) & !is.na(LC_Res) & is.na(MaxHt_Res))
sfluI.pred <- subset(rflu, !is.na(MaxDU_Res) & is.na(LC_Res) & is.na(MaxHt_Res))

# DU/acre to be handled on the log scale
#sflu[, logMaxDU_Res := log(MaxDU_Res)]
#fitlnoout <- lm(logMaxDU_Res ~ log(MaxHt_Res), data = sflu)
# remove outliers and keep only unique records
#sflu <- subset(sflu, !(MaxHt_Res > 50 & MaxDU_Res < 9 | log(MaxDU_Res) < 0))
sflu <- subset(sflu, log(MaxDU_Res) > 0)
sflu <- unique(sflu[, .(MaxHt_Res, MaxDU_Res, LC_Res, rural)])
sflu[, logMaxDU_Res := log(MaxDU_Res)]
# subset to be used in the estimation if lot coverage is not missing
sflu1 <- subset(sflu, !is.na(LC_Res))

# library(BMA)
# bma <- bic.glm(data.frame(height = sflu1$MaxHt_Res,
#                           lheight = log(sflu1$MaxHt_Res), coverage = sflu1$LC_Res,
#                           lcoverage = log(sflu1$LC_Res), rural = sflu1$rural),
#                sflu1$logMaxDU_Res, glm.family = gaussian())
# summary(bma)
# model with highest probability: lheight + coverage + rural

# estimation of DU/acre using heights, coverage and rural
fitall <- lm(logMaxDU_Res ~ log(MaxHt_Res) + LC_Res + I(rural), data = sflu1)
summary(fitall)

# estimation of DU/acre using heights and rural only
fitlt <- lm(logMaxDU_Res ~ log(MaxHt_Res) + I(rural), data = sflu)
summary(fitlt)

with(sflu, plot(log(MaxHt_Res), logMaxDU_Res))
with(sflu[is.na(rural) | rural == FALSE], plot(log(MaxHt_Res), logMaxDU_Res))
abline(fitlt)


result <- list(a = fitall$coefficients[1], b = fitall$coefficients[2], 
               c = fitall$coefficients[3], q = fitall$coefficients[4],
               d = fitlt$coefficients[1], e = fitlt$coefficients[2], 
               r = fitlt$coefficients[3])
result

stop("")
############################################
# Below are some diagnostics plots and experimentation code
############################################
# plot results, including test specifications
fitl <- lm(logMaxDU_Res ~ log(MaxHt_Res), data = sflu)
fitl.rural <- lm(logMaxDU_Res ~ -1 + I(rural), data = sflu)
fitl.urban <- lm(logMaxDU_Res ~ -1 + I(!rural), data = sflu)
fit.const <- lm(MaxDU_Res ~ -1 + MaxHt_Res, data = sflu)

plot(logMaxDU_Res ~ jitter(log(MaxHt_Res)), data = sflu, xlab = "log(max height)", ylab = "log(max DU/acre)")
abline(fitl$coefficients)
abline(fitl.urban$coefficients, lty = 2)
abline(fitl.rural$coefficients, lty = 3)


# impute test specification
sflu.pred[, MaxDU_ResPred := exp(fitl$coefficients[1] + fitl$coefficients[2]*log(MaxHt_Res))]


#pdf("dua_height_2026.pdf")
plot(MaxDU_Res ~ MaxHt_Res, data = sflu, xlab = "max height", ylab = "max DU/acre", 
     main = "DU/acre vs Height", 
     xlim = range(sflu$MaxHt_Res, min(max(sflu.pred$MaxHt_Res), 400))
     )
rng <- range(sflu[["MaxHt_Res"]], sflu.pred$MaxHt_Res)
rngarr <- rng[1]:rng[2]
lines(rngarr, (rngarr/15)^2) # previous imputation formula, for comparison here
lines(rngarr, exp(fitl$coefficients[1] + fitl$coefficients[2]*log(rngarr)), col = "red")
lines(rngarr, exp(fitl.urban$coefficients[1] + fitl.urban$coefficients[2]*log(rngarr)), col = "red", lty = 2)
lines(rngarr, exp(fitl.rural$coefficients[1] + fitl.rural$coefficients[2]*log(rngarr)), col = "red", lty = 3)
abline(c(0, fit.const$coefficients), col = "blue")
legend("topleft", legend = c("(H/15)^2", paste0(round(fit.const$coefficients[1], 2), "*H"), 
                             paste0("exp(", round(fitl$coefficients[1], 2), " + ", 
                                round(fitl$coefficients[2], 2), "*log(H))")), 
                             col = c("black", "blue", "red"), lty = 1, bty = "n")
points(subset(sflu.pred, rural == FALSE)$MaxHt_Res, rep(0, sum(!sflu.pred$rural)), col = "red")
points(subset(sflu.pred, rural == TRUE)$MaxHt_Res, rep(0, sum(sflu.pred$rural)), col = "orange")
#dev.off()


# plot results from the estimation model with lot coverage
fitl1 <- lm(logMaxDU_Res ~ log(MaxHt_Res) + log(LC_Res), data = sflu1) # rural is ignored here
sflu1.pred[, MaxDU_ResPred := exp(fitl1$coefficients[1] + fitl1$coefficients[2]*log(MaxHt_Res) + fitl1$coefficients[3]*log(LC_Res))]
plot(MaxDU_Res ~ MaxHt_Res, data = sflu, xlab = "max height", ylab = "max DU/acre", 
     main = "DU/acre vs Height", xlim = range(sflu$MaxHt_Res, min(max(sflu1.pred$MaxHt_Res), 600)))
rng <- range(sflu[["MaxHt_Res"]], sflu1.pred$MaxHt_Res)
rngarr <- rng[1]:rng[2]
lines(rngarr, exp(fitl$coefficients[1] + fitl$coefficients[2]*log(rngarr)), col = "red")
points(sflu1.pred$MaxHt_Res, sflu1.pred$MaxDU_ResPred, col = "red")

legend("topleft", legend = c(paste0("exp(", round(fitl$coefficients[1], 2), " + ", 
                                    round(fitl$coefficients[2], 2), "*log(H))"),
                            ), 
       col = c("black", "blue", "red"), lty = 1, bty = "n")


sfluI1.pred[, MaxHt_ResPred := exp((log(MaxDU_Res) - fitl1$coefficients[1] - fitl1$coefficients[3] * log(LC_Res))/fitl1$coefficients[2])]
sfluI.pred[, MaxHt_ResPred := exp((log(MaxDU_Res) - fitl$coefficients[1])/fitl$coefficients[2])]



#pdf("far_height_2026.pdf", width = 10, height = 8)
par(mfrow = c(2,2))
for(lutype in c("Comm", "Indust", "Office", "Mixed")) {
    height.attr <- paste0("MaxHt_", lutype)
    far.attr <- paste0("MaxFAR_", lutype)
    far.imp.attr <- paste0("MaxFAR_", lutype, "_imp")
    idx <- flu[[paste0(lutype, "_Use")]] == "Y" & !is.na(flu[[height.attr]]) & !is.na(flu[[far.imp.attr]])
    sflu <- flu[flu[[paste0(lutype, "_Use")]] == "Y" & !is.na(flu[[height.attr]]) & !is.na(flu[[paste0(far.attr)]]),]
    plot(sflu[[height.attr]], sflu[[far.attr]], xlab = "max height", ylab = "max FAR", main = lutype,
         ylim = range(sflu[[paste0(far.attr)]], flu[[paste0(far.imp.attr)]][idx], na.rm = TRUE))
    abline(c(0, 0.05))
    points(flu[[height.attr]][idx], flu[[far.imp.attr]][idx], col = "red")
    legend("topright", legend = c("observed", "imputed"), col = c("black", "red"), bty = "n", pch = 1)
}
#dev.off()



lutype <- "Res"
sflu <- flu[flu[[paste0(lutype, "_Use")]] == "Y" & !is.na(flu[[paste0("MaxHt_", lutype)]]) & !is.na(flu[[paste0("MaxDU_", lutype)]]),]
sflu<- sflu[Mixed_Use == "N"]
plot(sflu[["MaxHt_Res"]], sflu[["MaxDU_Res"]], xlab = "max height", ylab = "max DU/acre", main = "DU/acre vs Height")


# checking
rflu[MinDU_Res > MaxDU_Res]
rflu[!is.na(ResDU_lot)][order(-ResDU_lot)]
