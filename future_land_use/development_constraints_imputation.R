# Impute development constraints and compare to previous density table
# Latest version of script (formerly 'development_constraints_imputation_b.R')

library(stringr)
library(purrr)
library(magrittr)
library(data.table)
library(foreign)
library(openxlsx)

# Setup -------------------------------------------------------------------

# in.path <- "C:/Users/clam/Desktop/urbansim-baseyear-prep/future_land_use"
in.path <- "J:/Staff/Christy/usim-baseyear/flu"
out.path <- "C:/Users/clam/Desktop/urbansim-baseyear-prep/future_land_use"
master.lookup <- "Full_FLU_Master_Corres_File.xlsx"

# read new FLU and old FLU files
flu <- fread("W:/gis/projects/compplan_zoning/density_table_4_gis.csv")
oflu <- read.dbf("X:/DSA/FutureLandUse/FLU_2016/FLU_for_DC.dbf", as.is = TRUE) %>% as.data.table
lu <- read.xlsx(file.path(in.path, master.lookup)) %>% as.data.table

# missing Mixed
id.cols <- c(str_subset(colnames(flu), "^Juris|Zone"), "Key", "Definition")
use.cols <- str_subset(colnames(flu), "^[R|C|O|I].*_Use$")
max.cols <- c(str_subset(colnames(flu), "^MaxD.*_[R].*"), str_subset(colnames(flu), "^MaxF.*_[C|O|I].*"))
maxht.cols <- str_subset(colnames(flu), "^MaxHt.*_[R|C|O|I].*")
lc.cols <- str_subset(colnames(flu), "^LC_[R|C|O|I].*")

# organize column names into sets of lists
# an iterator for loops
cols.sets <- pmap(list(use.cols, max.cols, maxht.cols, lc.cols), list) %>% 
  map(~set_names(.x, c("use", "dens", "height", "lc"))) %>% 
  set_names(c("Res", "Comm", "Office", "Indust"))

# add mixed use to cols.sets
cols.sets[['Mixed']] <- list(use = "Mixed_Use", 
                             dens = list(du = "MaxDU_Mixed", far = "MaxFAR_Mixed"),
                             height = "MaxHt_Mixed",
                             lc = "LC_Mixed")

# Clean FLU ---------------------------------------------------------------

clean.cols <- c(maxht.cols, max.cols, lc.cols, cols.sets$Mixed$dens, cols.sets$Mixed$height, cols.sets$Mixed$lc, 'MaxFAR_Res')
for (col in unlist(clean.cols)) {
  # convert cols to double type
  if(is.character(flu[[col]]))
    flu[[col]] <- as.double(flu[[col]])
}

flu[, rural := ifelse(startsWith(rural, "rural"), TRUE, FALSE)]

# Compile previous flu ----------------------------------------------------


# clean old flu (2016)
oflu[, Max_Height := as.numeric(Max_Height)]

oflu.max.cols <- str_subset(colnames(oflu), "^Max_.*")
oflu.cols <- c("Jurisdicti", "Key", "Definition", oflu.max.cols)
oflu <- oflu[, ..oflu.cols]


# Clean lookup ------------------------------------------------------------


# attach master id to flu and oflu
lu.flu <- lu[, .(FLU_master_ID, Key = FLUadj_Key)]
flu <- merge(flu, lu.flu, all.x = TRUE, by = 'Key')

lu.oflu <- lu[, .(FLU_master_ID, Key = FLU16_Key)]
oflu <- merge(oflu, lu.oflu, all.x = TRUE, by = 'Key')


# Join flu to old flu -----------------------------------------------------


# _new = flu, _prev = old flu (2016)
# create union; see what joins and what doesn't
flu.join <- merge(flu, oflu, by = c("FLU_master_ID"), suffixes = c("_new", "_prev"), all = TRUE)
setnames(flu.join, oflu.max.cols, paste0(oflu.max.cols, "_prev"))


# Collected & previous values -------------------------------------------


flu.imp <- copy(flu.join)

## densities ----

# update col ending '_imp' with prev du/far if available and copy original Max du/far to '_imp' cols
for (i in 1:length(cols.sets)) {
  print(names(cols.sets[i]))
  s <- cols.sets[[i]]
  
  for (j in s$dens) {
    use.col <-s$use
    
    ifelse(str_detect(j, "DU"), prev.dens.col <- "Max_DU_Ac_prev", prev.dens.col <- "Max_FAR_prev")
    
    if (names(cols.sets[i]) == "Mixed") {
      ifelse(str_detect(j, "DU"), density.col <- s$dens$du, density.col <- s$dens$far)
    } else {
      density.col <- s$dens
    }
    
    imp.density.col <- paste0(density.col, "_imp")
    newcolnm_tag <- paste0(j, "_src")
    prev.equat <- parse(text = paste0("\`:=\`(", imp.density.col, "= ", prev.dens.col, ",", newcolnm_tag, "= 'prev')"))
    orig.equat <- parse(text = paste0("\`:=\`(", imp.density.col, "= ", density.col, ",", newcolnm_tag, "= 'collected')"))
    
    # update col ending '_imp' with original du/far
    flu.imp[!is.na(Jurisdicti_new) &
              get(eval(use.col)) == "Y" &
              (get(eval(density.col)) > 0), eval(orig.equat)]
    
    # update col ending '_imp' with prev du/far
    flu.imp[!is.na(Jurisdicti_new) & # is not null (flu.imp is a union)
              get(eval(use.col)) == "Y" &
              is.na(get(eval(density.col))) &
              (get(eval(prev.dens.col)) > 0), eval(prev.equat)]
  }
}

## residential max height ----

# For 'Res' and 'Mixed' types, update col ending '_imp' with prev MaxHt if available
## option to copy original Max du/far to '_imp' cols
for (stype in c('Res', 'Mixed')) {
  s <- cols.sets[[stype]]
  
  use.col <-s$use
  ht.col <- s$height
  
  prev.ht.col <- "Max_Height_prev"
  imp.ht.col <- paste0(ht.col, "_imp")
  newcolnm_tag <- paste0(ht.col, "_src")
  
  prev.equat <- parse(text = paste0("\`:=\`(", imp.ht.col, "= ", prev.ht.col, ",", newcolnm_tag, "= 'prev')"))
  orig.equat <- parse(text = paste0("\`:=\`(", imp.ht.col, "= ", ht.col, ",", newcolnm_tag, "= 'collected')"))
  
  # update col ending '_imp' with collected height
  flu.imp[!is.na(Jurisdicti_new) &
            get(eval(use.col)) == "Y" &
            (get(eval(ht.col)) > 0), eval(orig.equat)]
  
  # update col ending '_imp' with prev height
  flu.imp[!is.na(Jurisdicti_new) &
            get(eval(use.col)) == "Y" &
            is.na(get(eval(imp.ht.col))) &
            is.na(get(eval(ht.col))) &
            (get(eval(prev.dens.col)) > 0), eval(prev.equat)]
  

}


# Impute values -----------------------------------------------------------


# adjust MaxHt_Res column prior to imputation (retain original)
flu[, MaxHt_Res_orig := MaxHt_Res]
adj1 <- with(flu, !is.na(MaxFAR_Res) & !is.na(MaxFAR_Mixed) & MaxFAR_Res < MaxFAR_Mixed & MaxFAR_Res > 0)
adj2 <- with(flu, !is.na(MaxFAR_Res) & !is.na(MaxFAR_Mixed) & MaxFAR_Res > MaxFAR_Mixed)
flu[adj1, MaxHt_Res := round(MaxHt_Res * MaxFAR_Res/MaxFAR_Mixed)]
flu[adj2, MaxHt_Res := round(MaxHt_Res * MaxFAR_Res/(MaxFAR_Mixed + MaxFAR_Res))]

# coefficients
coeff <- list(a = 1.403, b = 0.654, c = 2.121, q = -0.980, d = -2.880, e = 1.448, r = -2.187)

# Impute max DU/ac, residential height, and FAR
# Update 'Max_XXX_imp' columns with imputed values if criteria is met and tag 'Max_XXX_src' column as 'imputed'
for (i in 1:length(cols.sets)) {
  print(names(cols.sets[i]))
  
  for (j in cols.sets[[i]]$dens) {
    print(j)
    
    use.col <-cols.sets[[i]]$use
    ht.col <- cols.sets[[i]]$height
    lc.col <- cols.sets[[i]]$lc
    
    newcolnm <- paste0(j, "_imp")
    newcolnm_tag <- paste0(j, "_src")
    
    newcolnm.ht <- paste0(ht.col, "_imp")
    newcolnm_tag.ht <- paste0(ht.col, "_src")
    
    # density columns (switch for Mixed Use)
    if (names(cols.sets[i]) == "Mixed") {
      ifelse(str_detect(j, "DU"), density.col <- cols.sets[[i]]$dens$du, density.col <- cols.sets[[i]]$dens$far)
    } else {
      density.col <- cols.sets[[i]]$dens
    }
    
    if (str_detect(j, "DU")) {
      
      if (j == "MaxDU_Res") {
        # Records with non-missing MaxFAR_Res and missing MaxDU_Res. 
        # Update original MaxDU_Res column and _imp column.
        convert.far.du <-  "43560 * MaxFAR_Res/1000"
        equat0 <- parse(text = paste0("\`:=\`(", density.col, " = ", convert.far.du,",", 
                                      newcolnm, "= ", convert.far.du, ",",
                                      newcolnm_tag, "= 'imputed')"))
        flu.imp[get(eval(use.col)) == "Y" &
                  is.na(MaxDU_Res) & 
                  !is.na(MaxFAR_Res) &
                  is.na(get(eval(newcolnm))),
                eval(equat0)]
      }
      
      # Records with missing DU/acre, non-missing heights and non-missing lot coverage(LC)
      # DU/acre = exp(a + b*log(height) + c*log(LC) + q*I(rural))
      equat1 <- parse(text = paste0("\`:=\`(", newcolnm, "= (exp(",coeff$a," + ",
                                    coeff$b,"*log(", ht.col, ") + ",
                                    coeff$c,"*log(", lc.col, ") + ",
                                    coeff$q, "*I(rural))),",
                                    newcolnm_tag, "= 'imputed')"))
      flu.imp[get(eval(use.col)) == "Y" &
            (is.na(get(eval(density.col))) | get(eval(density.col)) == 0) &
            !is.na(get(eval(ht.col))) &
            !is.na(get(eval(lc.col))) &
              is.na(get(eval(newcolnm))), eval(equat1)]

      # Records with missing DU/acre, non-missing heights and missing lot coverage
      # DU/acre = exp(d + e*log(height)+ r*I(rural))
      equat2 <- parse(text = paste0("\`:=\`(", newcolnm, "= (exp(", coeff$d ,"+",
                                    coeff$e,"*log(", ht.col, ") + ",
                                    coeff$r, "*I(rural))),",
                                    newcolnm_tag, "= 'imputed')"))
      flu.imp[get(eval(use.col)) == "Y" &
            (is.na(get(eval(density.col))) | get(eval(density.col)) == 0) &
            !is.na(get(eval(ht.col))) & 
              is.na(get(eval(lc.col))) &
              is.na(get(eval(newcolnm))), eval(equat2)]

      # Records with missing height, non-missing DU/acre and non-missing lot coverage:
      # height = exp[(log(DU/acre) - a - c*log(LC) - q*I(rural))/b]
      equat3 <- parse(text = paste0("\`:=\`(", newcolnm.ht, "= (exp((log(", density.col,") - ",
                                    coeff$a,"-",
                                    coeff$c,"*log(", lc.col,") -",
                                    coeff$r, "*I(rural))/",
                                    coeff$b ,")),",
                                    newcolnm_tag.ht, "= 'imputed')"))
      flu.imp[get(eval(use.col)) == "Y" &
            (is.na(get(eval(ht.col))) | get(eval(ht.col)) == 0) &
            (!is.na(get(eval(density.col))) | get(eval(density.col)) != 0) &
            (!is.na(get(eval(lc.col))) | get(eval(lc.col)) != 0), eval(equat3)]

      # Records with missing height, non-missing DU/acre and missing lot coverage:
      # height = exp[(log(DU/acre) - d - r*I(rural))/e]
      equat4 <- parse(text = paste0("\`:=\`(", newcolnm.ht, "= (exp((log(", density.col,")-",
                                    coeff$d," -",
                                    coeff$r, "*I(rural))/",
                                    coeff$e,")),",
                                    newcolnm_tag.ht, "= 'imputed')"))
      flu.imp[get(eval(use.col)) == "Y" &
            (is.na(get(eval(ht.col))) | get(eval(ht.col)) == 0) &
            (!is.na(get(eval(density.col))) | get(eval(density.col)) != 0) &
            (is.na(get(eval(lc.col))) | get(eval(lc.col)) == 0) &
              is.na(get(eval(newcolnm.ht))), eval(equat4)]

    } else {
      equat <- parse(text = paste0("\`:=\`(", newcolnm, "= ", ht.col, "/20,", newcolnm_tag, "= 'imputed')"))
      flu.imp[get(eval(use.col)) == "Y" &
            (is.na(get(eval(density.col))) | get(eval(density.col)) == 0) &
            !is.na(get(eval(ht.col))) &
              is.na(get(eval(newcolnm))), eval(equat)]
    }
  }
}


# exclude _prev records that didn't match current flu records
flu.fin.prep <- flu.imp[!is.na(Jurisdicti_new)]

## temp output for QC (kitchen sink file) ----
fwrite(flu.fin.prep, file.path(out.path, paste0("temp_flu_imputed_", Sys.Date(), ".csv")))

# Final output ------------------------------------------------------------


# subset columns and rename in preparation for 'unroll_constraints' .py script
ff.types <- c("Res", "Mixed", "Office", "Indust", "Comm")
ff.max.cols <- c(paste0("MaxDU_", ff.types), paste0("MaxFAR_", ff.types))
ff.excl.cols <- c(str_subset(colnames(flu.fin.prep), "_prev"), ff.max.cols, 
                  "MaxHt_Res", "MaxHt_Mixed", "MaxHt_Res_orig") # add MaxHt cols

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
