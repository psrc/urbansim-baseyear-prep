# Script for estimating coefficients to impute DU/acre and heights
# Hana Sevcikova (PSRC)
# 06/21/2021


library(data.table)
setwd("~/psrc/FLU/BY2018")

# read the FLU file
flu <- fread("density_table_4_gis.csv") # taken from W:\gis\projects\compplan_zoning

# clean MaxFAR_Res & MaxFAR_Mixed
flu[MaxFAR_Res == "", MaxFAR_Res := NA]
flu[MaxFAR_Mixed == "", MaxFAR_Mixed := NA]
#flu[MaxFAR_Res == "4 for 55', 5.5 for 85'", MaxFAR_Res := 5.5]
flu[MaxFAR_Res == "residential exempt from FAR", MaxFAR_Res := NA]
flu[MaxFAR_Mixed == "residential exempt from FAR", MaxFAR_Mixed := NA]
flu[, `:=`(MaxFAR_Res = as.numeric(MaxFAR_Res), MaxFAR_Mixed = as.numeric(MaxFAR_Mixed))] # convert to numeric
flu[, rural := ifelse(startsWith(rural, "rural"), TRUE, FALSE)] # make the rural column consistent

# Subset of residential records
rflu <- subset(flu, Res_Use == "Y")

# adjust height
rflu[, MaxHt_Res_orig := MaxHt_Res]
# additive FAR in Redmond, Tacoma and Bainbridge
adj1 <- with (rflu, !is.na(MaxFAR_Res) & !is.na(MaxFAR_Mixed) & MaxFAR_Res < MaxFAR_Mixed & MaxFAR_Res > 0)
adj2 <- with (rflu, !is.na(MaxFAR_Res) & !is.na(MaxFAR_Mixed) & MaxFAR_Res > MaxFAR_Mixed)
rflu[adj1, MaxHt_Res := round(MaxHt_Res * MaxFAR_Res/MaxFAR_Mixed)]
rflu[adj2, MaxHt_Res := round(MaxHt_Res * MaxFAR_Res/(MaxFAR_Mixed + MaxFAR_Res))]

# subset of residential records that will get DU/acre imputed
rflu.pred1 <- subset(rflu, is.na(MaxDU_Res))
# summarize those records
rflu.pred1[, .N, by = .(hasLC = !is.na(LC_Res), hasHt = !is.na(MaxHt_Res), hasFAR = !is.na(MaxFAR_Res)#, hasFARmx = !is.na(MaxFAR_Mixed)
                        )][order(hasLC, hasHt, hasFAR, decreasing = TRUE)][]

# subset of residential records to be used in the estimation, i.e. DU/acre is not missing
rflu.est <- subset(rflu, !is.na(MaxDU_Res))
# summarize those records
rflu.est[, .N, by = .(hasLC = !is.na(LC_Res), hasHt = !is.na(MaxHt_Res), hasFAR = !is.na(MaxFAR_Res))][order(hasLC, hasHt, hasFAR, decreasing = TRUE)][]
rflu.est[, .N, by = .(hasLC = !is.na(LC_Res), hasHt = !is.na(MaxHt_Res), rural = rural)][order(hasLC, hasHt, decreasing = TRUE)][]
# rule of thumb: 20DU for every 1 FAR

# the estimation set needs to be reduced to those records that have non-missing height
sflu <- subset(rflu.est, !is.na(MaxHt_Res))

# records to be imputed grouped depending if they have lot coverage missing and the imputing attribute:
# to be used for imputation of DU/acre from heights
sflu1.pred <- subset(rflu, !is.na(MaxHt_Res) & !is.na(LC_Res) & is.na(MaxDU_Res))
sflu.pred <- subset(rflu, !is.na(MaxHt_Res) & is.na(LC_Res) & is.na(MaxDU_Res))

# to be used for imputation of heights from DU/acre
sfluI1.pred <- subset(rflu, !is.na(MaxDU_Res) & !is.na(LC_Res) & is.na(MaxHt_Res))
sfluI.pred <- subset(rflu, !is.na(MaxDU_Res) & is.na(LC_Res) & is.na(MaxHt_Res))

# DU/acre to be handled on the log scale
sflu[, logMaxDU_Res := log(MaxDU_Res)]
#fitlnoout <- lm(logMaxDU_Res ~ log(MaxHt_Res), data = sflu)
# remove outliers and keep only unique records
#sflu <- subset(sflu, !(MaxHt_Res > 50 & MaxDU_Res < 9 | log(MaxDU_Res) < 0))
#sflu <- subset(sflu, log(MaxDU_Res) > 0)
#sflu <- unique(sflu[, .(MaxHt_Res, MaxDU_Res, LC_Res)])
#sflu[, logMaxDU_Res := log(MaxDU_Res)]

# subset to be used in the estimation if lot coverage is not missing
sflu1 <- subset(sflu, !is.na(LC_Res))

#library(BMA)
#bma <- bic.glm(data.frame(lheight = log(sflu1$MaxHt_Res), coverage = sflu1$LC_Res, 
#                          lcoverage = log(sflu1$LC_Res)), sflu1$logMaxDU_Res, 
#               glm.family = gaussian())
#summary(bma)

# estimation of DU/acre using heights and rural only
fitlt <- lm(logMaxDU_Res ~ log(MaxHt_Res) + I(rural), data = sflu)
summary(fitlt)

# estimation of DU/acre using heights, lot coverage and rural
fitl1t <- lm(logMaxDU_Res ~ log(MaxHt_Res) + log(LC_Res) + I(rural), data = sflu1)
summary(fitl1t)

result <- list(a = fitl1t$coefficients[1], b = fitl1t$coefficients[2], c = fitl1t$coefficients[3], q = fitl1t$coefficients[4],
               d = fitlt$coefficients[1], e = fitlt$coefficients[2], r = fitlt$coefficients[3])
result

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

pdf("dua_height.pdf")
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
dev.off()


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



pdf("far_height.pdf", width = 10, height = 8)
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
dev.off()



lutype <- "Res"
sflu <- flu[flu[[paste0(lutype, "_Use")]] == "Y" & !is.na(flu[[paste0("MaxHt_", lutype)]]) & !is.na(flu[[paste0("MaxDU_", lutype)]]),]
sflu<- sflu[Mixed_Use == "N"]
plot(sflu[["MaxHt_Res"]], sflu[["MaxDU_Res"]], xlab = "max height", ylab = "max DU/acre", main = "DU/acre vs Height")
