# Clean 2026 FLU table and impute various measures.
# Adapted from development_constraints_imputation.R (originally written by Christy Lam)
# Hana Sevcikova (PSRC)
# 04/22/2026

library(stringr)
library(purrr)
library(magrittr)
library(data.table)
library(foreign)
library(openxlsx)

in.path <- "~/psrc/urbansim-baseyear-prep/future_land_use"
#in.path <- "J:/Staff/Christy/usim-baseyear/flu"
out.path <- in.path
#out.path <- "C:/Users/clam/Desktop/urbansim-baseyear-prep/future_land_use"

master.lookup <- file.path(in.path, "data2026", "Full_FLU_Master_Corres_File.xlsx")
new.flu.name <- file.path(in.path, "data2026", "Zoning_2026_d2.xlsx")
old.flu.name <- file.path(in.path, "data", "final_flu_postprocessed_2023-01-10.csv")
#old.flu.name <- file.path(in.path, "density_table_4_gis.csv")

# read new FLU and old FLU files
#fluall <- fread(new.flu.name)
fluall <- data.table(read_xlsx(new.flu.name))
ofluall <- fread(old.flu.name)
lu <- read.xlsx(master.lookup) %>% as.data.table

# extract bonus/no-bonus records
flubonus <- fluall[Bonus_included == "Y"]
flunobonus <- fluall[Bonus_included == ""]
flu <- rbind(flubonus, flunobonus[!juris_zn %in% flubonus$juris_zn])[Zone != "ERROR"]

# collect columnn names
id.cols <- intersect(colnames(flu), c(str_subset(colnames(flu), "^Juris|Zone"), "Key", "Definition"))
use.cols <- str_subset(colnames(flu), "^[R|C|O|I|M].*_Use$")
max.cols <- c(str_subset(colnames(flu), "^MaxD.*_[R].*"), str_subset(colnames(flu), "^MaxF.*_[C|O|I|M].*"))
min.cols <- c(str_subset(colnames(flu), "^MinD.*_[R].*"), str_subset(colnames(flu), "^MinF.*_[C|O|I|M].*"))
maxht.cols <- str_subset(colnames(flu), "^MaxHt.*_[R|C|O|I|M].*")
lc.cols <- str_subset(colnames(flu), "^LC_[R|C|O|I|M].*")

# organize column names into sets of lists
# an iterator for loops
cols.sets <- pmap(list(use.cols, min.cols, max.cols, maxht.cols, lc.cols), list) %>% 
  map(~set_names(.x, c("use", "min_dens", "max_dens", "height", "lc"))) %>% 
  set_names(c("Res", "Comm", "Office", "Indust", "Mixed"))

# add missing density items
cols.sets[['Mixed']][['max_dens']] <- list(du = "MaxDU_Mixed", far = cols.sets[['Mixed']][['max_dens']])
cols.sets[['Mixed']][['min_dens']] <- list(du = "MinDU_Mixed", far = cols.sets[['Mixed']][['min_dens']])
cols.sets[['Res']][['max_dens']] <- list(du = cols.sets[['Res']][['max_dens']], far = "MaxFAR_Res", lot = "ResDU_lot")
cols.sets[['Res']][['min_dens']] <- list(du = cols.sets[['Res']][['min_dens']], far = "MinFAR_Res")


# Clean FLU ---------------------------------------------------------------

clean.cols <- unique(c(maxht.cols, min.cols, max.cols, lc.cols, 
                       unlist(cols.sets$Mixed[c('max_dens', 'min_dens')]),
                       unlist(cols.sets$Res[c('max_dens', 'min_dens')])))

# which columns should be considered to identify duplicates
cols.for.dupl <- c(clean.cols, use.cols, "Juris", "juris_zn", "rural")
# check for duplicates
flu[, .N, by = "juris_zn"][order(-N)]
nflu <- nrow(flu)
flu <- flu[!duplicated(flu, by = cols.for.dupl)] # removes duplicates
cat("\nRemoved ", nflu - nrow(flu), " duplicate rows.\n")

for (col in clean.cols) {
  # clean various numeric columns (set "None" to NA & "unlimited" to -1)
  if(is.character(flu[[col]]))
    flu[get(col) %in% c("", "None"), (col) := NA]
    flu[get(col) == "unlimited", (col) := -1]   
}

# fix Ht (some records have info in terms of stories, e.g. "3 story")
for(col in maxht.cols)
  flu[grepl(" story", get(col)), (col) := as.integer(gsub(" story", "", get(col))) * 12]

# there are some values in parentheses, remove it (but double check if it makes sense)
#flu[,.N, by = "MaxFAR_Mixed"]
for(col in clean.cols){
  flu[, (col) := gsub("\\s*\\([^\\)]+\\)", "", get(col))]
}

# In Sumner MaxDU_Res there are values defined as ranges, take the middle
col <- "MaxDU_Res"
flu[, c("rng1", "rng2") := tstrsplit(get(col), "-", type.convert = TRUE)]
flu[!is.na(rng2), c(col, "juris_zn", "rng1", "rng2"), with = FALSE] # view affected rows
flu[!is.na(rng2), (col) := round(rng1 + (rng2 - rng1)/2)][, `:=`(rng1 = NULL, rng2 = NULL)]

for (col in clean.cols) {
  # convert cols to double type
  # if there is a warning, investigate in which column and why, 
  # by setting options(warn = 2) & flu[,.N, by = col]
  if(is.character(flu[[col]]))
    flu[ , (col) := as.double(get(col))]
}

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
flu[, rural := ifelse(rural == "Y", TRUE, ifelse(rural %in% c("N", "not"), FALSE, NA))]
# set various zones as "rural" (20 records)
# found via flu[grepl("rural", Definition) & is.na(rural)]
flu[(Zone %in% c("FL", "EPF-RAN", "RIC", "RNC", "RSR", "RR", "RW", "RCO", "SVLR", "FRL", "ARL", "RSep") |
       juris_zn %in% c("Pierce_County_GC", "Kenmore_GC", "Kenmore_P", "Kent_A-10", "Kitsap_RP",
                       "Kitsap_RI", "Kitsap_REC", "Kitsap_TTEC")) 
    & is.na(rural), rural := TRUE]
# set NA rural records to FALSE 
flu[is.na(rural), rural := FALSE]

# set to residential use if ResDU_lot or MaxDU_Res given
flu[!is.na(ResDU_lot) & ResDU_lot > 100, ResDU_lot := NA] # these records seem to contain sqft (and not DU) in this field 
flu[((!is.na(ResDU_lot)  & ResDU_lot > 0)  | (!is.na(MaxDU_Res) & MaxDU_Res > 0)) & Res_Use != "Y", Res_Use := "Y"]
# Check Auburn_R-3 (ResDU_lot = 40) Should it be MaxDU_Res?

paste(sort(setdiff(unique(flu[Res_Use == "Y" & (is.na(rural) | rural == TRUE), Juris]), 
        flu[!is.na(ResDU_lot)  & ResDU_lot %in% 2:6, .N, by = "Juris"][, Juris])), collapse = ", ")

# if "missing middle" is in the description but neither ResDU_lot nor MaxDU_Res given,
# set ResDU_lot to -1 (i.e. follow the HC1110 law). It applies to two zones in Marysville
flu[grepl("missing middle", Definition) & Res_Use == "Y" & is.na(ResDU_lot) & is.na(MaxDU_Res),
    `:=`(ResDU_lot = -1, ResDU_lot_src = 'imputed')]

# if use-specific LC not given but LC_Mixed given, set the use-specific LC to that
for(use in c("Res", "Comm", "Office", "Indust")){
  flu[get(paste0(use, "_Use")) == "Y" & is.na(get(paste0("LC_", use))) & !is.na(LC_Mixed),
      (paste0("LC_", use)) := LC_Mixed]
}

# for residential records without MaxDU_Res, MaxFAR_Res, ResDU_lot but with MaxDU_Mixed, use that for MaxDU_Res
flu[Res_Use == "Y" & is.na(MaxDU_Res) & is.na(MaxFAR_Res) & is.na(ResDU_lot) & !is.na(MaxDU_Mixed), 
    `:=`(MaxDU_Res = MaxDU_Mixed, MaxDU_Res_src = 'imputed')]
# similarly for missing MinDU_Res - take it from MinDU_Mixed
flu[Res_Use == "Y" & !is.na(MinDU_Mixed) & is.na(ResDU_lot) & (is.na(MinDU_Res) | MinDU_Res < MinDU_Mixed), 
    `:=`(MinDU_Res = MinDU_Mixed, MinDU_Res_src = 'imputed')]

# for residential records without MaxHt_Res but with MaxHt_Mixed, use that for MaxHt_Res
flu[Res_Use == "Y" & is.na(MaxHt_Res) & !is.na(MaxHt_Mixed), 
    `:=`(MaxHt_Res = MaxHt_Mixed, MaxDU_Res_src = 'imputed')]

# for mobile home parks with missing DU/acre, impute 5
flu[Zone == "MHP" & Res_Use == "Y" & is.na(MaxDU_Res) & is.na(ResDU_lot), 
    `:=`(MaxDU_Res = 5, MaxDU_Res_src = 'imputed')]

# convert FAR to DU/acre
# first compute floors to get efficiency (from ChatGPT)
idx <- with(flu, Res_Use == "Y" & !is.na(MaxFAR_Res) & MaxFAR_Res > 0 & is.na(MaxDU_Res) & is.na(ResDU_lot))
flu[idx & !is.na(MaxHt_Res), floors := round(MaxHt_Res/12)]
flu[idx, eff := ifelse(is.na(floors) | floors < 12, 0.8, 0.7)]
# for MF (if it allows Mixed use), use 800sf per unit
flu[idx & Mixed_Use == "Y", MaxDU_Res := round(MaxFAR_Res * 43560 * eff / 800)]
# for SF (if Mixed use is not allowed), use 1000sf per unit
flu[idx & Mixed_Use != "Y", MaxDU_Res := round(MaxFAR_Res * 43560 * eff / 1000)]
flu[idx, MaxDU_Res_src := "imputed"]
flu[, `:=`(floors = NULL, eff = NULL)]

# something simpler and more conservative for MinFAR_Res -> MinDU_Res
flu[Res_Use == "Y" & !is.na(MinFAR_Res) & MinFAR_Res > 0 & is.na(MinDU_Res) & is.na(ResDU_lot),
    `:=`(MinDU_Res = pmin(round(MinFAR_Res * 43560 * 0.8 / 1200), MaxDU_Res, na.rm = TRUE),
         MinDU_Res_src = 'imputed')]

# check which records are only Mixed use and nothing else
unique(flu[Mixed_Use == "Y" & Res_Use != "Y" & Comm_Use != "Y" & Indust_Use != "Y" & Office_Use != "Y" & grepl("residential", Definition), Juris])


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
               get(use.col) == "Y" &
               !is.na(get(new.dens.col)) & get(new.dens.col) > 0, eval(orig.equat)]

    # update col ending '_imp' with prev du/far
    flu.imp[!is.na(Juris_new) & # is not null (flu.imp is a union)
              get(use.col) == "Y" &
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
             get(use.col) == "Y" &
             !is.na(get(new.ht.col)) & get(new.ht.col) > 0, eval(orig.equat)]
   
  # update col ending '_imp' with prev height
  prev.equat <- parse(text = paste0("\`:=\`(", imp.ht.col, "= ", prev.ht.col, ",", newcolnm_tag, "= 'prev')"))

  flu.imp[!is.na(Juris_new) &
            get(eval(use.col)) == "Y" &
            is.na(get(imp.ht.col)) &
            is.na(get(new.ht.col)) &
            !is.na(get(prev.ht.col)) & get(prev.ht.col) > 0, eval(prev.equat)]

 }
 
# Impute values -----------------------------------------------------------

flu.imp <- flu.imp[!is.na(Juris_new)]
 
# adjust MaxHt_Res column prior to imputation (retain original)
flu.imp[, MaxHt_Res_orig := MaxHt_Res_imp]
adj1 <- with(flu.imp, !is.na(MaxFAR_Res) & !is.na(MaxFAR_Mixed_imp) & MaxFAR_Res < MaxFAR_Mixed_imp & MaxFAR_Res > 0)
adj2 <- with(flu.imp, !is.na(MaxFAR_Res) & !is.na(MaxFAR_Mixed_imp) & MaxFAR_Res > MaxFAR_Mixed_imp)
flu.imp[adj1, MaxHt_Res_imp := round(MaxHt_Res_imp * MaxFAR_Res/MaxFAR_Mixed_imp)]
flu.imp[adj2, MaxHt_Res_imp := round(MaxHt_Res_imp * MaxFAR_Res/(MaxFAR_Mixed_imp + MaxFAR_Res))]

# coefficients
#coeff <- list(a = 1.403, b = 0.654, c = 2.121, q = -0.980, d = -2.880, e = 1.448, r = -2.187)
coeff <- list(a = -1.856659, b = 1.004703, c = 0.016398, q = -0.865158, 
              d = -2.5284, e = 1.4307, r = -0.9589)


no.info.rows <- flu.imp[Res_Use == "Y" & is.na(LC_Res) & is.na(MaxHt_Res_imp) & 
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
      
      flu.imp[get(use.col) == "Y" & is.na(ResDU_lot) &
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

      flu.imp[get(use.col) == "Y"  & is.na(ResDU_lot) &
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
      flu.imp[get(use.col) == "Y" & 
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
      flu.imp[get(eval(use.col)) == "Y" &
            (is.na(get(newcolnm.ht)) | get(newcolnm.ht) == 0) &
            (!is.na(get(density.col)) | get(density.col) != 0) &
            (is.na(get(lc.col)) | get(lc.col) == 0), eval(equat4)]

    } else {
      equat <- parse(text = paste0("\`:=\`(", newcolnm, "= ", newcolnm.ht, "/20,", newcolnm_tag, "= 'imputed')"))
      stop("")
      flu.imp[get(eval(use.col)) == "Y" &
            (is.na(get(eval(density.col))) | get(eval(density.col)) == 0) &
            !is.na(get(eval(newcolnm.ht))) &
              is.na(get(eval(newcolnm))), eval(equat)]
      
      # impute height for non-res use
      # height = pmax(12, 12*FAR/lot_coverage)
      # missing lot_coverage fill in with 1
      flu.imp[get(eval(use.col)) == "Y" &
                (is.na(get(eval(newcolnm.ht))) | get(eval(newcolnm.ht)) == 0) &
                is.na(get(eval(lc.col))), (lc.col) := 1]
      
      equat.nonres.ht <- parse(text = paste0("\`:=\`(", newcolnm.ht, "= pmax(12, 12*", newcolnm,"/", lc.col,"), ", newcolnm_tag.ht, "= 'imputed')"))
      flu.imp[get(eval(use.col)) == "Y" &
                (is.na(get(eval(newcolnm.ht))) | get(eval(newcolnm.ht)) == 0), eval(equat.nonres.ht)]
    }
  }
}


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
