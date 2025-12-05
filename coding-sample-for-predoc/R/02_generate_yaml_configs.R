# 02_generate_yaml_configs.R
#
# Goal:
#   From CPS household (hh) and person (pp) files, build one YAML
#   configuration file per household as input to the benefit simulator.
#
#   - Filter to valid Type A households with 1–12 persons
#   - Re-order persons so adults come first
#   - Build wide person variables: age / disabled / blind / ssdi
#   - Create household flags: married, prev_ssi
#   - Normalize final_location to "Place, ST" (with special CT handling)
#   - Write configs/household_<row_id>.yml
#
# NOTE:
#   This is a code-only sample. The CPS input files are not included in
#   the public repo.

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(yaml)
  library(glue)
  library(fs)
  library(purrr)
  library(data.table)
})

# helper: normalize CT towns to "X town"
normalize_ct_town <- function(place_vec) {
  base <- stringr::str_trim(
    stringr::str_replace(place_vec, "\\s+(city|town)\\s*$", "")
  )
  paste0(base, " town")
}

# ---- main function ----------------------------------------------------

generate_yaml_configs <- function(hh_file,
                                  pp_file,
                                  cfg_dir = "configs") {
  
  # 1. Read CPS household & person files
  #    Keep IDs as character to avoid issues with leading zeros.
  hh <- read_csv(
    hh_file,
    col_types = cols(
      H_IDNUM = col_character(),
      row_id  = col_integer(),   # produced upstream in the geography step
      .default = col_guess()
    )
  )
  
  pp <- read_csv(
    pp_file,
    col_types = cols(
      PERIDNUM  = col_character(),
      P_SEQ     = col_integer(),
      A_LINENO  = col_integer(),
      A_AGE     = col_integer(),
      DIS_YN    = col_integer(),
      PEDISEYE  = col_integer(),
      DSAB_VAL  = col_integer(),
      A_FAMREL  = col_integer(),
      A_MARITL  = col_integer(),
      .default  = col_guess()
    )
  )
  
  # 1a. Filter out invalid households (Type A, residential, non-empty, etc.)
  hh_clean <- hh %>%
    filter(
      H_TYPEBC == 0,          # Type A households only
      HRHTYPE   %in% 1:8,     # residential household types 01–08
      H_NUMPER  > 0,          # at least one person
      H_NUMPER  <= 12,        # cap at 12 persons
      HHSTATUS != 0,          # exclude invalid households
      HEFAMINC != -1,         # income screener universe
      H_RESPNM != 0           # valid respondent
    )
  
  # 1b. Person file: keep only persons in hh_clean & P_SEQ <= 12
  pp_clean <- pp %>%
    filter(
      substr(PERIDNUM, 1, 20) %in% hh_clean$H_IDNUM,
      P_SEQ <= 12
    ) %>%
    mutate(
      H_IDNUM = substr(PERIDNUM, 1, 20)
    )
  
  # 1c. Within each household, put adults first, then preserve original order
  pp_clean <- pp_clean %>%
    group_by(H_IDNUM) %>%
    arrange(
      dplyr::desc(A_AGE >= 18),  # adults first
      P_SEQ                      # then original CPS sequence
    ) %>%
    mutate(
      P_SEQ_NEW = dplyr::row_number()
    ) %>%
    ungroup()
  
  # 1d. Append state abbreviations to final_location
  state_map <- tibble(
    state_full = c(state.name, "District of Columbia"),
    state_abbr = c(state.abb, "DC")
  )
  
  hh_clean2 <- hh_clean %>%
    left_join(state_map, by = c("final_state_name" = "state_full")) %>%
    mutate(
      final_location = dplyr::case_when(
        state_abbr == "CT" ~ stringr::str_c(
          normalize_ct_town(final_location), ", ", state_abbr
        ),
        !is.na(state_abbr) ~ stringr::str_c(
          stringr::str_trim(final_location), ", ", state_abbr
        ),
        TRUE ~ final_location
      )
    ) %>%
    select(-state_abbr)
  
  # 2. Build person-wide table (up to 12 persons per household)
  persons_wide <- pp_clean %>%
    transmute(
      H_IDNUM,
      p_seq    = P_SEQ_NEW,
      age      = A_AGE,
      disabled = as.integer(DIS_YN == 1),
      blind    = as.integer(PEDISEYE == 1),
      ssdi     = DSAB_VAL
    ) %>%
    pivot_wider(
      id_cols     = H_IDNUM,
      names_from  = p_seq,
      values_from = c(age, disabled, blind, ssdi),
      names_glue  = "{.value}{p_seq}"
    )
  
  # 3. Married flag: if any reference person (A_FAMREL==1) is married (A_MARITL==1)
  married_flag <- pp_clean %>%
    filter(A_FAMREL == 1) %>%
    transmute(H_IDNUM, married = as.integer(A_MARITL == 1)) %>%
    distinct()
  
  married_flag_dt <- data.table(married_flag)
  setorder(married_flag_dt, H_IDNUM, -married)
  married_flag <- married_flag_dt[, .SD[1], keyby = H_IDNUM]
  
  # 4. Join everything back to household level
  hh_full <- hh_clean2 %>%
    left_join(persons_wide,  by = "H_IDNUM") %>%
    left_join(married_flag,  by = "H_IDNUM") %>%
    mutate(
      married  = dplyr::coalesce(married, 0L),
      prev_ssi = as.integer(HSSI_YN == 1)
    )
  
  # 5. Ensure configs directory exists
  dir_create(cfg_dir)
  
  # 6. Static fields (policy switches, grids, etc.)
  static_fields <- list(
    ruleYear               = list(2025L),
    Year                   = list(2025L),
    income_start           = 0L,
    income_end             = 500000L,
    income_increase_by     = 1000L,
    income.investment      = list(0L),
    income.gift            = list(0L),
    income.child_support   = list(0L),
    APPLY_CHILDCARE        = TRUE,
    APPLY_CCDF             = TRUE,
    APPLY_HEALTHCARE       = TRUE,
    APPLY_TANF             = TRUE,
    APPLY_HEADSTART        = TRUE,
    APPLY_PREK             = TRUE,
    APPLY_LIHEAP           = TRUE,
    APPLY_MEDICAID_ADULT   = TRUE,
    APPLY_MEDICAID_CHILD   = TRUE,
    APPLY_ACA              = TRUE,
    APPLY_SECTION8         = TRUE,
    APPLY_RAP              = FALSE,
    APPLY_FRSP             = FALSE,
    APPLY_SNAP             = TRUE,
    APPLY_SLP              = TRUE,
    APPLY_WIC              = TRUE,
    APPLY_EITC             = TRUE,
    APPLY_TAXES            = TRUE,
    APPLY_CTC              = TRUE,
    APPLY_CDCTC            = TRUE,
    APPLY_FATES            = TRUE,
    APPLY_SSI              = TRUE,
    APPLY_SSDI             = TRUE,
    k_ftorpt               = "FT",
    schoolagesummercare    = "PT",
    headstart_ftorpt       = "PT",
    preK_ftorpt            = "PT",
    contelig.headstart     = FALSE,
    contelig.earlyheadstart= FALSE,
    contelig.ccdf          = FALSE,
    budget.ALICE           = "survivalforcliff",
    assets.car1            = 0L
  )
  
  # 7. Write one YAML per household (indexed by row_id)
  walk2(hh_full$row_id, split(hh_full, hh_full$row_id), ~{
    id  <- .x
    row <- .y
    
    dyn <- list(
      household_id    = row$H_IDNUM,
      
      # Ages
      agePerson1      = list(row$age1),   agePerson2  = list(row$age2),
      agePerson3      = list(row$age3),   agePerson4  = list(row$age4),
      agePerson5      = list(row$age5),   agePerson6  = list(row$age6),
      agePerson7      = list(row$age7),   agePerson8  = list(row$age8),
      agePerson9      = list(row$age9),   agePerson10 = list(row$age10),
      agePerson11     = list(row$age11),  agePerson12 = list(row$age12),
      
      married         = list(row$married),
      prev_ssi        = list(row$prev_ssi),
      
      # Disabilities
      disability1     = list(row$disabled1),  disability2  = list(row$disabled2),
      disability3     = list(row$disabled3),  disability4  = list(row$disabled4),
      disability5     = list(row$disabled5),  disability6  = list(row$disabled6),
      disability7     = list(row$disabled7),  disability8  = list(row$disabled8),
      disability9     = list(row$disabled9),  disability10 = list(row$disabled10),
      disability11    = list(row$disabled11), disability12 = list(row$disabled12),
      
      # Blindness
      blind1          = list(row$blind1),  blind2 = list(row$blind2),
      blind3          = list(row$blind3),  blind4 = list(row$blind4),
      blind5          = list(row$blind5),  blind6 = list(row$blind6),
      
      # SSDI
      ssdiPIA1        = list(row$ssdi1),   ssdiPIA2 = list(row$ssdi2),
      ssdiPIA3        = list(row$ssdi3),   ssdiPIA4 = list(row$ssdi4),
      ssdiPIA5        = list(row$ssdi5),   ssdiPIA6 = list(row$ssdi6),
      
      # Geography & flags
      locations       = list(row$final_location),
      empl_healthcare = list(row$NOW_HCOV),
      ownorrent       = list(if (row$H_TENURE == 1) "own" else "rent"),
      `assets.cash`   = list(row$HFINVAL),
      `disab.work.exp`= list(row$HDISVAL)
    )
    
    cfg     <- c(static_fields, dyn)
    yml_txt <- as.yaml(cfg)
    
    out_path <- file.path(cfg_dir, glue("household_{id}.yml"))
    write_file(yml_txt, file = out_path)
  })
  
  invisible(hh_full)
}

# If the script is run directly (not sourced), provide a simple example call.
if (sys.nframe() == 0) {
  # Example placeholders: these files are NOT in the public repo,
  # but this shows how the function would be called.
  generate_yaml_configs(
    hh_file = "output/hhpub24_with_imputed_geography_fixed.csv",
    pp_file = "data-raw/pppub24.csv",
    cfg_dir = "configs"
  )
}
