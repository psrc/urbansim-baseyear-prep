# Clean 2026 FLU table and impute various measures, using coefficients 
# estimated via the estimation_FLU2026.R script.
# Adapted from development_constraints_imputation.R (originally written by Christy Lam)
# Hana Sevcikova (PSRC)
# 04/22/2026

library(stringr)
library(purrr)
library(magrittr)
library(data.table)
library(foreign)
library(readxl)

in.path <- "~/psrc/urbansim-baseyear-prep/future_land_use"
#in.path <- "J:/Staff/Christy/usim-baseyear/flu"
out.path <- in.path
#out.path <- "C:/Users/clam/Desktop/urbansim-baseyear-prep/future_land_use"

master.lookup <- file.path(in.path, "data2026", "Full_FLU_Master_Corres_File.xlsx")
new.flu.name <- file.path(in.path, "data2026", "Zoning_2026_d3.xlsx")
old.flu.name <- file.path(in.path, "data", "final_flu_postprocessed_2023-01-10.csv")
#old.flu.name <- file.path(in.path, "density_table_4_gis.csv")

# read new FLU and old FLU files
source("load_FLU2026.R")
ofluall <- fread(old.flu.name)
lu <- read_xlsx(master.lookup) %>% as.data.table

# set the source of the density columns to "collected"
for (i in 1:length(cols.sets)) {
  s <- cols.sets[[i]]
  use.col <-s$use
  for (density.col in s$max_dens) {
    colnm_tag <- paste0(density.col, "_src")
    equat <- parse(text = paste0("\`:=\`(", colnm_tag, " = 'collected')"))
    flu[!is.na(get(density.col)) & get(density.col) > 0, eval(equat)]
  }
  colnm_tag <- paste0(s$height, "_src")
  equat <- parse(text = paste0("\`:=\`(", colnm_tag, " = 'collected')"))
  flu[!is.na(get(s$height)) & get(s$height) > 0, eval(equat)]
}


flu[, .N, by = "rural"] # very few records have this info
process_rural(flu)

more_cleaning(flu)


paste(sort(setdiff(unique(flu[Res_Use == TRUE & (is.na(rural) | rural == TRUE), Juris]), 
        flu[!is.na(ResDU_lot)  & ResDU_lot %in% 2:6, .N, by = "Juris"][, Juris])), collapse = ", ")


# if use-specific Height not given but MaxHt_Mixed given, set the use-specific height to that
for(use in c("Res", "Comm", "Office", "Indust")){
  equat <- parse(text = paste0("\`:=\`( MaxHt_", use, " = MaxHt_Mixed, MaxHt_",  
                               use, "_src = 'asserted')"))
  flu[get(paste0(use, "_Use")) == TRUE & is.na(get(paste0("MaxHt_", use))) & !is.na(MaxHt_Mixed),
      eval(equat)]
}

# for residential records without MaxDU_Res, MaxFAR_Res, ResDU_lot but with MaxDU_Mixed, use that for MaxDU_Res
flu[Res_Use == TRUE & is.na(MaxDU_Res) & is.na(MaxFAR_Res) & is.na(ResDU_lot) & !is.na(MaxDU_Mixed) & MaxDU_Mixed != 0, 
    `:=`(MaxDU_Res = MaxDU_Mixed, MaxDU_Res_src = 'asserted')]

# similarly for missing MinDU_Res - take it from MinDU_Mixed
flu[Res_Use == TRUE & !is.na(MinDU_Mixed) & is.na(ResDU_lot) & (is.na(MinDU_Res) | MinDU_Res < MinDU_Mixed), 
    `:=`(MinDU_Res = MinDU_Mixed, MinDU_Res_src = 'asserted')]

# for mobile home parks with missing DU/acre, impute 5
flu[Zone == "MHP" & Res_Use == TRUE & is.na(MaxDU_Res) & is.na(ResDU_lot), 
    `:=`(MaxDU_Res = 5, MaxDU_Res_src = 'asserted')]

# convert FAR to DU/acre
# first compute floors to get efficiency (from ChatGPT)
idx <- with(flu, Res_Use == TRUE & !is.na(MaxFAR_Res) & MaxFAR_Res > 0 & is.na(MaxDU_Res) & is.na(ResDU_lot))
flu[idx & !is.na(MaxHt_Res), floors := round(MaxHt_Res/12)]
flu[idx, eff := ifelse(is.na(floors) | floors < 12, 0.8, 0.7)]
# for MF (if it allows Mixed use), use 800sf per unit
flu[idx & Mixed_Use == TRUE, MaxDU_Res := round(MaxFAR_Res * 43560 * eff / 800)]
# for SF (if Mixed use is not allowed), use 1000sf per unit
flu[idx & Mixed_Use == FALSE, MaxDU_Res := round(MaxFAR_Res * 43560 * eff / 1000)]
flu[idx, MaxDU_Res_src := "estimated"]
flu[, `:=`(floors = NULL, eff = NULL)]

# something simpler and more conservative for MinFAR_Res -> MinDU_Res
flu[Res_Use == TRUE & !is.na(MinFAR_Res) & MinFAR_Res > 0 & is.na(MinDU_Res) & is.na(ResDU_lot),
    `:=`(MinDU_Res = pmin(round(MinFAR_Res * 43560 * 0.8 / 1200), MaxDU_Res, na.rm = TRUE),
         MinDU_Res_src = 'estimated')]

# check which records are only Mixed use and nothing else
unique(flu[Mixed_Use == TRUE & Res_Use == FALSE & Comm_Use == FALSE & Indust_Use == FALSE & Office_Use == FALSE & grepl("residential", Definition), Juris])


# Compile previous flu ----------------------------------------------------

# clean old flu (2019)
oflu.max.cols <- str_subset(colnames(ofluall), "^Max.*")
oflu.max.cols <- setdiff(oflu.max.cols, str_subset(colnames(ofluall), "_src"))
oflu.cols <- intersect(colnames(ofluall), 
                       c("Jurisdicti", "Key", "Juris_zn", "Definition", "edit", oflu.max.cols))
oflu <- ofluall[, ..oflu.cols]
setnames(oflu, c("Jurisdicti", "Juris_zn"), c("Juris", "juris_zn"))

# Join with lookup ------------------------------------------------------------

# # attach master id to flu and oflu
flu <- merge(flu, lu[, .(FLU_master_id, Key = juris_zn_26)], all.x = TRUE, 
             by.x = "juris_zn", by.y = "Key")
# 
lu.oflu <- lu[, .(FLU_master_id, Key = juris_zn_19)]
oflu <- merge(oflu, lu.oflu, all.x = TRUE, by.x = "juris_zn", by.y = "Key")
# 
# 
# Join flu to old flu -----------------------------------------------------
# 
# 
# _new = flu, _prev = old flu (2019)
# create union; see what joins and what doesn't
# TODO: deal with the duplicates in the flu object!!!
flu.join <- merge(flu[!duplicated(FLU_master_id)], oflu, by = c("FLU_master_id"), 
                  suffixes = c("_new", "_prev"), all = TRUE)

# Collected & previous values -------------------------------------------

flu.imp <- copy(flu.join)

## densities ----
 
# update col ending '_imp' with prev du/far if available and copy original Max du/far to '_imp' cols
for (i in 1:length(cols.sets)) {

   print(names(cols.sets[i]))
   s <- cols.sets[[i]]
   use.col <-s$use
   
   for (density.col in s$max_dens) {
     if(!density.col %in% colnames(oflu)) next
     prev.dens.col <- paste0(density.col, "_prev")
     new.dens.col <- paste0(density.col, "_new")
     
     imp.density.col <- paste0(density.col, "_imp")
     newcolnm_tag <- paste0(density.col, "_src")
     prev.equat <- parse(text = paste0("\`:=\`(", imp.density.col, " = ", prev.dens.col, ", ", newcolnm_tag, " = 'prev')"))
     orig.equat <- parse(text = paste0("\`:=\`(", imp.density.col, " = ", new.dens.col, ")"))

     # update col ending '_imp' with original du/far
     flu.imp[!is.na(Juris_new) &
               get(use.col) == TRUE &
               !is.na(get(new.dens.col)) & get(new.dens.col) > 0, eval(orig.equat)]

    # update col ending '_imp' with prev du/far
    flu.imp[!is.na(Juris_new) & # is not null (flu.imp is a union)
              get(use.col) == TRUE &
              (is.na(get(new.dens.col)) | get(new.dens.col) == 0) &
              !is.na(get(prev.dens.col)) & get(prev.dens.col) > 0, eval(prev.equat)]
   }
}
 
 cat("\n")

# ## Max height ----
# 
# # update original Max Ht to '_imp' cols
# # also copy prev MaxHt for 'Res' and 'Mixed' if available to col ending '_imp' 
# 
for (stype in names(cols.sets)) {
   s <- cols.sets[[stype]]
   
   use.col <-s$use
   ht.col <- s$height
   if(!ht.col %in% colnames(oflu)) next
   
   prev.ht.col <- paste0(ht.col, "_prev")
   new.ht.col <- paste0(ht.col, "_new")
   
   imp.ht.col <- paste0(ht.col, "_imp")
   newcolnm_tag <- paste0(ht.col, "_src")
   
   # update col ending '_imp' with collected height
   orig.equat <- parse(text = paste0("\`:=\`(", imp.ht.col, " = ", new.ht.col, ")"))
   
   flu.imp[!is.na(Juris_new) &
             get(use.col) == TRUE &
             !is.na(get(new.ht.col)) & get(new.ht.col) > 0, eval(orig.equat)]
   
  # update col ending '_imp' with prev height
  prev.equat <- parse(text = paste0("\`:=\`(", imp.ht.col, "= ", prev.ht.col, ",", newcolnm_tag, "= 'prev')"))

  flu.imp[!is.na(Juris_new) &
            get(eval(use.col)) == TRUE &
            is.na(get(imp.ht.col)) &
            is.na(get(new.ht.col)) &
            !is.na(get(prev.ht.col)) & get(prev.ht.col) > 0, eval(prev.equat)]

 }
 
# Impute values -----------------------------------------------------------

flu.imp <- flu.imp[!is.na(Juris_new)]
 
# adjust MaxHt_Res column prior to imputation (retain original)
flu.imp[, MaxHt_Res_orig := MaxHt_Res_imp] # TODO: is it needed? It should be in MaxHt_Res_new
adj1 <- with(flu.imp, !is.na(MaxFAR_Res) & !is.na(MaxFAR_Mixed_imp) & MaxFAR_Res < MaxFAR_Mixed_imp & MaxFAR_Res > 0)
adj2 <- with(flu.imp, !is.na(MaxFAR_Res) & !is.na(MaxFAR_Mixed_imp) & MaxFAR_Res > MaxFAR_Mixed_imp)
flu.imp[adj1, `:=`(MaxHt_Res_imp = round(MaxHt_Res_imp * MaxFAR_Res/MaxFAR_Mixed_imp), 
                   MaxHt_Res_src = 'adjusted')]
flu.imp[adj2, `:=`(MaxHt_Res_imp = round(MaxHt_Res_imp * MaxFAR_Res/(MaxFAR_Mixed_imp + MaxFAR_Res)),
                   MaxHt_Res_src = 'adjusted')]

# coefficients (estimated via estimation_FLU2026.R)
#coeff <- list(a = 1.403, b = 0.654, c = 2.121, q = -0.980, d = -2.880, e = 1.448, r = -2.187)
coeff <- list(a = -1.856659, b = 1.004703, c = 0.016398, q = -0.865158, 
              d = -2.5284, e = 1.4307, r = -0.9589)


no.info.rows <- flu.imp[Res_Use == TRUE & is.na(LC_Res) & is.na(MaxHt_Res_imp) & 
                          is.na(MaxFAR_Res) & is.na(MaxDU_Res_imp) & is.na(ResDU_lot)]

# Impute max DU/ac, height, and FAR
# Update 'Max_XXX_imp' columns with imputed values if criteria is met and tag 'Max_XXX_src' column as 'imputed'
for (i in 1:length(cols.sets)) {
  print(names(cols.sets[i]))
  
  use.col <-cols.sets[[i]]$use
  ht.col <- cols.sets[[i]]$height
  lc.col <- cols.sets[[i]]$lc
  
  for (j in cols.sets[[i]]$max_dens) {
    if(j %in% c("MaxFAR_Res", "ResDU_lot")) next
    print(j)

    newcolnm <- paste0(j, "_imp")
    newcolnm_tag <- paste0(j, "_src")
    
    newcolnm.ht <- paste0(ht.col, "_imp")
    newcolnm_tag.ht <- paste0(ht.col, "_src")
    
    # density columns (switch for Mixed Use)
    #if (names(cols.sets[i]) == "Mixed") {
    #  ifelse(str_detect(j, "DU"), density.col <- cols.sets[[i]]$dens$du, density.col <- cols.sets[[i]]$dens$far)
    #} else {
    #  density.col <- cols.sets[[i]]$dens
    #}
    
    density.col <- paste0(j, "_imp")

    if (str_detect(j, "DU") && !str_detect(j, "lot")) {
      
      # Records with missing DU/acre, non-missing heights and non-missing lot coverage(LC)
      # DU/acre = exp(a + b*log(height) + c*log(LC) + q*I(rural))
      equat1 <- parse(text = paste0("\`:=\`(", newcolnm, "= (exp(",coeff$a," + ",
                                    coeff$b,"*log(", newcolnm.ht, ") + ",
                                    coeff$c,"*log(", lc.col, ") + ",
                                    coeff$q, "*I(rural))),",
                                    newcolnm_tag, "= 'imputed')"))
      
      flu.imp[get(use.col) == TRUE & is.na(ResDU_lot) &
            (is.na(get(density.col)) | get(density.col) == 0) &
            !is.na(get(newcolnm.ht)) & get(newcolnm.ht) > 0 &
            !is.na(get(lc.col)) & get(lc.col) > 0 & 
              is.na(get(newcolnm)), eval(equat1)]

      # Records with missing DU/acre, non-missing heights and missing lot coverage
      # DU/acre = exp(d + e*log(height)+ r*I(rural))
      equat2 <- parse(text = paste0("\`:=\`(", newcolnm, "= (exp(", coeff$d ,"+",
                                    coeff$e,"*log(", newcolnm.ht, ") + ",
                                    coeff$r, "*I(rural))),",
                                    newcolnm_tag, "= 'imputed')"))

      flu.imp[get(use.col) == TRUE  & is.na(ResDU_lot) &
            (is.na(get(density.col)) | get(density.col) == 0) &
            !is.na(get(newcolnm.ht)) & 
              is.na(get(lc.col)) & get(lc.col) > 0 &
              is.na(get(newcolnm)), eval(equat2)]

      # Records with missing height, non-missing DU/acre and non-missing lot coverage:
      # height = exp[(log(DU/acre) - a - c*log(LC) - q*I(rural))/b]
      equat3 <- parse(text = paste0("\`:=\`(", newcolnm.ht, "= (exp((log(", density.col,") - ",
                                    coeff$a,"-",
                                    coeff$c,"*log(", lc.col,") -",
                                    coeff$r, "*I(rural))/",
                                    coeff$b ,")),",
                                    newcolnm_tag.ht, "= 'imputed')"))
      flu.imp[get(use.col) == TRUE & 
            (is.na(get(newcolnm.ht)) | get(newcolnm.ht) == 0) &
            (!is.na(get(density.col)) | get(density.col) != 0) &
            (!is.na(get(lc.col)) | get(lc.col) != 0), eval(equat3)]

      # Records with missing height, non-missing DU/acre and missing lot coverage:
      # height = exp[(log(DU/acre) - d - r*I(rural))/e]
      equat4 <- parse(text = paste0("\`:=\`(", newcolnm.ht, "= (exp((log(", density.col,")-",
                                    coeff$d," -",
                                    coeff$r, "*I(rural))/",
                                    coeff$e,")),",
                                    newcolnm_tag.ht, "= 'imputed')"))
      flu.imp[get(eval(use.col)) == TRUE &
            (is.na(get(newcolnm.ht)) | get(newcolnm.ht) == 0) &
            (!is.na(get(density.col)) | get(density.col) != 0) &
            (is.na(get(lc.col)) | get(lc.col) == 0), eval(equat4)]

    } else { # non-residential imputation
      # for missing lot coverage values, use its median 
      med_lc <- flu.imp[get(use.col) == TRUE & !is.na(get(lc.col)) & get(lc.col) > 0, median(get(lc.col))]
      flu.imp[get(use.col) == TRUE & is.na(get(lc.col)) | get(lc.col) == 0, (lc.col) := med_lc]

      # impute FAR for non-res use via
      # FAR = height * lot_coverage / 12
      equat <- parse(text = paste0("\`:=\`(", newcolnm, " = ", newcolnm.ht, 
                                   " * 0.01 * ", lc.col, " / 12, ", newcolnm_tag, "= 'imputed')"))
      flu.imp[get(use.col) == TRUE &
            (is.na(get(density.col)) | get(density.col) == 0) &
            !is.na(get(newcolnm.ht)) &
              is.na(get(newcolnm)), eval(equat)]
      
      # impute height for non-res use
      # height = pmax(12, 12*FAR/lot_coverage)
      equat.nonres.ht <- parse(text = paste0("\`:=\`(", newcolnm.ht, "= pmax(12, 12 * ", newcolnm,"/(", lc.col,"/100)), ", newcolnm_tag.ht, "= 'imputed')"))
      flu.imp[get(use.col) == TRUE & !is.na(get(newcolnm)) &
                (is.na(get(newcolnm.ht)) | get(newcolnm.ht) == 0), eval(equat.nonres.ht)]
      miss <- flu.imp[get(use.col) == TRUE & is.na(get(newcolnm))]
      cat("Missing info for ", nrow(miss),
          " records: ", paste(unique(miss[, juris_zn_new]), collapse = ", "), "\n")
    }
  }
}

stop("")
# exclude _prev records that didn't match current flu records
flu.fin.prep <- flu.imp[!is.na(Juris_new)]

## temp output for QC (kitchen sink file) ----
fwrite(flu.fin.prep, file.path(out.path, paste0("temp_flu_imputed_", Sys.Date(), ".csv")))

# Final output ------------------------------------------------------------


# subset columns and rename in preparation for 'unroll_constraints' .py script
ff.types <- c("Res", "Mixed", "Office", "Indust", "Comm")
ff.max.cols <- c(paste0("MaxDU_", ff.types), paste0("MaxFAR_", ff.types), paste0("MaxHt_", ff.types))
ff.excl.cols <- c(str_subset(colnames(flu.fin.prep), "_prev"), ff.max.cols,
                  "MaxHt_Res_orig")

ff.cols <- setdiff(colnames(flu.fin.prep), ff.excl.cols)
flu.fin.prep <- flu.fin.prep[, ..ff.cols] 

# remove '_imp' from col name
colnames(flu.fin.prep) <- str_trim(str_replace_all(colnames(flu.fin.prep), "_imp", ""))
colnames(flu.fin.prep) <- str_trim(str_replace_all(colnames(flu.fin.prep), "_new", ""))

# remove duplicate rows (in preparation for assigning plan_type_ids in unroll_constraints.py)
gb.cols <- setdiff(colnames(flu.fin.prep), "Zone_adj")
flu.fin <- unique(flu.fin.prep, by = gb.cols, fromLast = T)

## output for use with unroll_constraints.py ----
fwrite(flu.fin, file.path(out.path, paste0("final_flu_imputed_", Sys.Date(), ".csv")))
