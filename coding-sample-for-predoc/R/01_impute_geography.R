# 01_impute_geography.R
#
# Goal:
#   Impute county (GTCO) and human-readable locations for CPS households
#   using:
#     - county population data
#     - FIPS codes from tidycensus
#     - CBSA → county crosswalk
#     - special handling for Connecticut town → old county codes
#     - a careful state-level fallback that avoids already-used counties
#
# Output:
#   A household file with:
#     - final_GTCO        (imputed county code)
#     - final_state_name  (state name)
#     - final_county_name (county name)
#     - final_location    (town/county, depending on state)
#     - row_id            (sequential household id used downstream)
#
# NOTE:
#   This is a code-only sample. The CPS input files and crosswalks
#   are **not** included in the public repo.

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tidycensus)
  library(stringi)
  library(tidyr)
})

# main function

impute_household_geography <- function(pop_file,
                                       ct_town_file,
                                       cbsa_xwalk_file,
                                       hh_file,
                                       ne_pop_file,
                                       out_file) {
  # 1. Clean county population data
  pop_clean <- read_csv(
    pop_file,
    col_types = cols(
      `Geographic Area` = col_character(),
      pop               = col_character()
    ),
    locale   = locale(grouping_mark = ",")
  ) %>%
    transmute(
      population = parse_number(pop, locale = locale(grouping_mark = ",")),
      geo_name   = str_remove(`Geographic Area`, "^\\."),
      county_raw = word(geo_name, 1, sep = ", "),
      state_raw  = word(geo_name, 2, sep = ", "),
      county_key = stri_trans_general(county_raw, "latin-ascii") %>%
        str_to_lower() %>%
        str_remove_all("\\s(county|parish|borough|census area|municipality|planning region)$") %>%
        str_remove_all("[^a-z0-9]"),
      state_key  = str_remove_all(state_raw, "[^A-Za-z0-9]") %>%
        str_to_lower()
    )
  
  # 2. Build FIPS mapping keys (from tidycensus::fips_codes)
  data("fips_codes", package = "tidycensus")
  
  mapping <- fips_codes %>%
    mutate(
      county_key  = stri_trans_general(county, "latin-ascii") %>%
        str_to_lower() %>%
        str_remove_all("\\s(county|parish|borough|census area|municipality|planning region)$") %>%
        str_remove_all("[^a-z0-9]"),
      state_key   = str_remove_all(state_name, "[^A-Za-z0-9]") %>%
        str_to_lower(),
      state_code  = as.integer(state_code),
      county_code = as.integer(county_code)
    ) %>%
    distinct(state_key, county_key, .keep_all = TRUE) %>%
    select(state_key, county_key, state_code, county_code)
  
  # 3. Aggregate CT town populations to *old* counties
  tt <- read_csv(
    ct_town_file,
    col_types = cols(
      town             = col_character(),
      old_county_code  = col_integer(),
      new_county_code  = col_integer(),
      population       = col_integer()
    )
  )
  
  ct_county_pop <- tt %>%
    group_by(old_county_code) %>%
    summarize(
      population = sum(population, na.rm = TRUE),
      .groups    = "drop"
    ) %>%
    rename(GTCO = old_county_code) %>%
    mutate(GESTFIPS = 9L) %>%
    # attach county codes for CT (state_code == 9) to check consistency
    left_join(
      mapping %>% filter(state_code == 9L) %>% select(county_code),
      by = c("GTCO" = "county_code")
    ) %>%
    select(GESTFIPS, GTCO, population)
  
  # 4. Combine cleaned population with FIPS mapping
  #    and replace CT by the town-aggregated version
  pop_fips_final <- pop_clean %>%
    left_join(mapping, by = c("state_key", "county_key")) %>%
    rename(GESTFIPS = state_code, GTCO = county_code) %>%
    mutate(
      GESTFIPS = as.integer(GESTFIPS),
      GTCO     = if_else(is.na(GTCO), 0L, as.integer(GTCO))
    ) %>%
    filter(!is.na(GESTFIPS), !is.na(population)) %>%
    filter(GESTFIPS != 9L) %>%           # CT handled separately
    select(GESTFIPS, GTCO, population) %>%
    bind_rows(ct_county_pop)
  
  # 5. Load CBSA-to-county crosswalk
  cbsa_county <- read_csv(
    cbsa_xwalk_file,
    col_types = cols(
      cbsacode       = col_integer(),
      fipsstatecode  = col_integer(),
      fipscountycode = col_integer()
    )
  ) %>%
    rename(
      GTCBSA   = cbsacode,
      GESTFIPS = fipsstatecode,
      GTCO     = fipscountycode
    ) %>%
    distinct(GTCBSA, GESTFIPS, GTCO)
  
  # 6. Prepare minimal household flags from CPS hh file
  hh_min <- read_csv(
    hh_file,
    col_types = cols_only(
      GESTFIPS = col_integer(),
      GTCBSA   = col_integer(),
      GTCO     = col_integer()
    )
  ) %>%
    mutate(
      row_id     = row_number(),
      has_county = GTCO != 0,
      has_cbsa   = GTCBSA != 0
    )
  
  # 7. Reported CBSA (any nonzero GTCBSA in hhpub24)
  reported_cbsa <- hh_min %>%
    filter(has_cbsa) %>%
    distinct(GESTFIPS, GTCBSA)
  
  # 8. CBSA-based imputation
  
  # 8A. Counties already observed in this CBSA within hh data
  reported_cbsa_counties <- hh_min %>%
    filter(has_cbsa, has_county) %>%
    distinct(GESTFIPS, GTCBSA, GTCO)
  
  # 8B. Candidate counties per CBSA from crosswalk
  cbsa_candidates <- cbsa_county %>%
    left_join(
      reported_cbsa_counties %>% mutate(reported = TRUE),
      by = c("GESTFIPS", "GTCBSA", "GTCO")
    ) %>%
    mutate(reported = replace_na(reported, FALSE))
  
  # 8C. Special CT adjustment: map new county codes to old ones
  cbsa_ct <- cbsa_candidates %>%
    filter(GESTFIPS == 9L) %>%
    left_join(
      tt %>%
        distinct(new_county_code, old_county_code) %>%
        rename(GTCO_new = new_county_code, GTCO_old = old_county_code),
      by = c("GTCO" = "GTCO_new")
    ) %>%
    select(-GTCO) %>%
    rename(GTCO = GTCO_old)
  
  cbsa_final <- bind_rows(
    cbsa_candidates %>% filter(GESTFIPS != 9L),
    cbsa_ct
  ) %>%
    distinct(GESTFIPS, GTCBSA, GTCO, .keep_all = TRUE)
  
  # 8D. For each (state, CBSA), pick the most populous available county,
  #     preferring counties that are *not* already reported.
  impute_cbsa <- cbsa_final %>%
    left_join(pop_fips_final, by = c("GESTFIPS", "GTCO")) %>%
    filter(!is.na(population)) %>%
    group_by(GESTFIPS, GTCBSA) %>%
    group_modify(~{
      df <- .x
      sel <- if (any(!df$reported)) df %>% filter(!reported) else df
      sel %>% slice_max(population, n = 1)
    }) %>%
    ungroup() %>%
    rename(impute_cbsa = GTCO) %>%
    select(GESTFIPS, GTCBSA, impute_cbsa)
  
  # 9. State-level fallback when no CBSA information
  #
  #    - candidate counties = all in state
  #      minus any tied to a reported CBSA
  #      minus any already present with a county in hh data
  #    - choose the most populous candidate county
  #    - if none remain, fall back to the state's single largest county
  
  # 9A. All counties tied to any *reported* CBSA
  reported_cbsa_counties_all <- cbsa_county %>%
    semi_join(reported_cbsa, by = c("GESTFIPS", "GTCBSA")) %>%
    distinct(GESTFIPS, GTCO)
  
  # 9B. Counties already reported directly in hh data
  reported_counties <- hh_min %>%
    filter(has_county) %>%
    distinct(GESTFIPS, GTCO)
  
  # 9C. Universe of counties per state
  all_state_counties <- pop_fips_final %>%
    distinct(GESTFIPS, GTCO)
  
  # 9D. Candidate counties
  candidate_counties <- all_state_counties %>%
    anti_join(reported_cbsa_counties_all, by = c("GESTFIPS", "GTCO")) %>%
    anti_join(reported_counties,          by = c("GESTFIPS", "GTCO"))
  
  # 9E. Attach population
  candidate_pop <- candidate_counties %>%
    left_join(pop_fips_final, by = c("GESTFIPS", "GTCO")) %>%
    filter(!is.na(population))
  
  # 9F. First-best: largest county among candidates
  state_impute1 <- candidate_pop %>%
    group_by(GESTFIPS) %>%
    slice_max(population, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    select(GESTFIPS, impute_state = GTCO)
  
  # 9G. Fallback: for states with no candidates, use state's largest county
  all_states     <- pop_fips_final %>% pull(GESTFIPS) %>% unique()
  missing_states <- setdiff(all_states, state_impute1$GESTFIPS)
  
  state_impute2 <- pop_fips_final %>%
    filter(GESTFIPS %in% missing_states) %>%
    group_by(GESTFIPS) %>%
    slice_max(population, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    select(GESTFIPS, impute_state = GTCO)
  
  # 9H. Final state-level fallback table
  impute_state <- bind_rows(state_impute1, state_impute2)
  
  # 10. Build final imputation table for each CPS row
  final_tbl <- hh_min %>%
    left_join(impute_cbsa,  by = c("GESTFIPS", "GTCBSA")) %>%
    left_join(impute_state, by = "GESTFIPS") %>%
    mutate(
      imputed_GTCO = dplyr::case_when(
        has_county                     ~ GTCO,
        has_cbsa & !is.na(impute_cbsa) ~ impute_cbsa,
        TRUE                           ~ impute_state
      )
    ) %>%
    select(row_id, imputed_GTCO)
  
  # 11. Largest CT towns & New England largest towns
  ct_town_max <- tt %>%
    group_by(old_county_code) %>%
    slice_max(population, n = 1) %>%
    ungroup() %>%
    select(old_county_code, town)
  
  ne_states <- c(23L, 25L, 33L, 44L, 50L)  # ME, MA, NH, RI, VT
  
  ne_town_pop <- read_csv(
    ne_pop_file,
    col_types = cols(.default = "c")
  ) %>%
    transmute(
      GESTFIPS  = as.integer(GESTFIPS),
      GTCO      = as.integer(GTCO),
      town_name = cityntown_raw,
      population = as.numeric(population)
    ) %>%
    filter(GESTFIPS %in% ne_states)
  
  ne_largest_town <- ne_town_pop %>%
    group_by(GESTFIPS, GTCO) %>%
    slice_max(population, n = 1) %>%
    ungroup() %>%
    select(GESTFIPS, GTCO, town_name)
  
  # 12. Fallback official county & state names
  global_fips <- fips_codes %>%
    transmute(
      GESTFIPS       = as.integer(state_code),
      GTCO           = as.integer(county_code),
      official_county = county,
      official_state  = state_name
    )
  
  # 13. Assemble final household dataset
  full_hh <- read_csv(hh_file) %>%
    mutate(row_id = row_number())
  
  final_output <- full_hh %>%
    left_join(final_tbl,        by = "row_id") %>%
    left_join(pop_fips_final,   by = c("GESTFIPS", "imputed_GTCO" = "GTCO")) %>%
    left_join(ct_town_max,      by = c("imputed_GTCO" = "old_county_code")) %>%
    left_join(ne_largest_town,  by = c("GESTFIPS", "imputed_GTCO" = "GTCO")) %>%
    left_join(global_fips,      by = c("GESTFIPS", "imputed_GTCO" = "GTCO")) %>%
    mutate(
      final_GTCO        = imputed_GTCO,
      final_state_name  = official_state,
      final_county_name = official_county,
      final_location    = dplyr::case_when(
        GESTFIPS == 9L                ~ town,       # CT: town name
        GESTFIPS %in% ne_states       ~ town_name,  # NE: town name
        TRUE                          ~ final_county_name
      )
    )
  
  # 14. Write results
  write_csv(final_output, out_file)
  
  invisible(final_output)
}

# If run directly (Rscript 01_impute_geography.R), show an example call.
if (sys.nframe() == 0) {
  # Example paths – these files are NOT included in the public repo.
  impute_household_geography(
    pop_file       = "data-raw/co-est2024-pop.csv",
    ct_town_file   = "data-raw/ct_town_county_old_new_codes_with_pop.csv",
    cbsa_xwalk_file= "data-raw/cbsa2fipsxw.csv",
    hh_file        = "data-raw/hhpub24.csv",
    ne_pop_file    = "data-raw/ne_pop_fips_final.csv",
    out_file       = "output/hhpub24_with_imputed_geography_fixed.csv"
  )
}
