# 03_run_distributed.R
#
# Goal:
#   Run the benefit calculator in parallel (one household per YAML config)
#   using future.batchtools + Slurm.
#
#   - Read per-household YAML configs from `configs/`
#   - Initialize the benefit-calculation environment (parameters + functions)
#   - For each household:
#       * build ALICE expense grid
#       * apply benefits (TANF / SSI / SSDI / SNAP / housing / healthcare / etc.)
#       * apply taxes and credits (EITC, CTC, CDCTC)
#       * compute derived variables (AfterTaxIncome, NetResources, etc.)
#   - Run in parallel on a Slurm cluster with error handling and chunked output
#
# NOTE:
#   - This is a code-only sample. The parameter .rdata files and
#     `libraries.R` / `functions/*.R` are not included in this repo.
#   - YAML configs are produced upstream by 02_generate_yaml_configs.R.

suppressPackageStartupMessages({
  library(yaml)
  library(data.table)
  library(compiler)
  library(future)
  library(future.apply)
  library(future.batchtools)
})

data.table::setDTthreads(1L)
options(future.delete = TRUE)

# 1) Environment initialization

init_env <- function(root_dir = getwd()) {
  # Load parameter RData into the global environment
  load(file.path(root_dir, "prd_parameters/expenses.rdata"),             envir = globalenv())
  load(file.path(root_dir, "prd_parameters/benefit.parameters.rdata"),   envir = globalenv())
  load(file.path(root_dir, "prd_parameters/tables.rdata"),               envir = globalenv())
  load(file.path(root_dir, "prd_parameters/parameters.defaults.rdata"),  envir = globalenv())
  
  # Unpack defaults into .GlobalEnv
  if (exists("parameters.defaults", envir = globalenv())) {
    list2env(get("parameters.defaults", envir = globalenv()), envir = globalenv())
  }
  
  # Load function scripts into .GlobalEnv (not included in this repo)
  source(file.path(root_dir, "libraries.R"),                               local = FALSE)
  source(file.path(root_dir, "functions/benefits_functions.R"),            local = FALSE)
  source(file.path(root_dir, "functions/expense_functions.R"),             local = FALSE)
  source(file.path(root_dir, "functions/BenefitsCalculator_functions.R"),  local = FALSE)
  source(file.path(root_dir, "functions/TANF.R"),                          local = FALSE)
  source(file.path(root_dir, "functions/CCDF.R"),                          local = FALSE)
  
  # JIT-compile hot functions
  hot_funcs <- c(
    "function.createData", "BenefitsCalculator.ALICEExpenses",
    "BenefitsCalculator.OtherBenefits", "BenefitsCalculator.Childcare",
    "BenefitsCalculator.Healthcare", "BenefitsCalculator.FoodandHousing",
    "BenefitsCalculator.TaxesandTaxCredits", "function.createVars"
  )
  for (fn in hot_funcs) {
    if (exists(fn, mode = "function")) {
      assign(fn, compiler::cmpfun(get(fn)), envir = .GlobalEnv)
    }
  }
  
  assign(".__INIT_DONE__", TRUE, .GlobalEnv)
  invisible(TRUE)
}

# 2) Helpers

`%||%`    <- function(x, y) if (is.null(x)) y else x
scalarize <- function(x) {
  if (is.list(x) && length(x) == 1L && !is.list(x[[1L]])) x[[1L]] else x
}

# Normalize NAs for inspection (not used in core pipeline, but handy)
sanitize_df <- function(x) {
  stopifnot(is.data.frame(x))
  dt <- data.table::as.data.table(data.table::copy(x))
  for (nm in names(dt)) {
    col <- dt[[nm]]
    if (is.logical(col)) {
      data.table::set(dt, j = nm, value = ifelse(is.na(col), FALSE, col))
    } else if (is.character(col)) {
      data.table::set(dt, j = nm, value = ifelse(is.na(col), "", col))
    } else if (is.numeric(col)) {
      data.table::set(dt, j = nm, value = ifelse(is.na(col), 0, col))
    }
  }
  dt
}

# Food & Housing with LIHEAP-safe fallback
safe_FoodAndHousing <- function(df, flags, log_file) {
  tryCatch(
    BenefitsCalculator.FoodandHousing(
      df, flags$SEC8, flags$LIHEAP, flags$SNAP, flags$SLP,
      flags$WIC, flags$RAP, flags$FRSP
    ),
    error = function(e) {
      # Log to file
      try(
        write(
          sprintf(
            "[%s] LIHEAP errored for state=%s; fallback to FALSE. Reason: %s",
            format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
            paste(na.omit(unique(df$stateAbbrev)), collapse = ","),
            e$message
          ),
          file = log_file,
          append = TRUE
        ),
        silent = TRUE
      )
      # Retry with LIHEAP turned off
      BenefitsCalculator.FoodandHousing(
        df, flags$SEC8, FALSE, flags$SNAP, flags$SLP,
        flags$WIC, flags$RAP, flags$FRSP
      )
    }
  )
}

# Flatten YAML lists into a single row
flatten_list <- function(x, parent = NULL) {
  out <- list()
  for (nm in names(x)) {
    val <- x[[nm]]
    key <- if (is.null(parent)) nm else paste(parent, nm, sep = ".")
    if (is.list(val) && !is.null(names(val))) {
      out <- c(out, flatten_list(val, key))
    } else if (is.atomic(val) && length(val) > 1L) {
      vec_names <- paste0(key, "_", seq_along(val))
      out[vec_names] <- as.list(val)
    } else {
      out[[key]] <- if (is.null(val)) NA else val
    }
  }
  out
}

as_chr_id <- function(v) {
  if (is.null(v)) return(NA_character_)
  if (is.list(v) && length(v) == 1L) v <- v[[1L]]
  if (is.character(v)) return(v)
  if (is.numeric(v))  return(format(v, scientific = FALSE, trim = TRUE))
  as.character(v)
}

# 3) Single-household runner
#    (expects globals: yaml_list, cols, out_dir, .__INIT_DONE__)

run_one <- function(hh_id) {
  stopifnot(exists("yaml_list"), !is.null(yaml_list[[hh_id]]), exists("cols"))
  if (!exists(".__INIT_DONE__", envir = .GlobalEnv)) {
    # Root dir assumed to be working directory
    init_env(getwd())
  }
  
  # Scalarize single-element lists from YAML
  inputs <- lapply(yaml_list[[hh_id]], scalarize)
  
  # Some functions refer to this global
  budget.ALICE <<- inputs$budget.ALICE %||% "survival"
  
  # Base dataframe + ALICE expenses
  df <- function.createData(inputs)
  df$household_id <- hh_id
  df <- BenefitsCalculator.ALICEExpenses(df)
  
  # Switches (defaults if missing)
  flags <- list(
    TANF      = inputs$APPLY_TANF        %||% TRUE,
    SSI       = inputs$APPLY_SSI         %||% TRUE,
    SSDI      = inputs$APPLY_SSDI        %||% TRUE,
    CHILDCARE = inputs$APPLY_CHILDCARE   %||% TRUE,
    HEADSTART = inputs$APPLY_HEADSTART   %||% TRUE,
    PREK      = inputs$APPLY_PREK        %||% TRUE,
    CCDF      = inputs$APPLY_CCDF        %||% TRUE,
    FATES     = inputs$APPLY_FATES       %||% FALSE,
    HEALTH    = inputs$APPLY_HEALTHCARE  %||% TRUE,
    MED_ADULT = inputs$APPLY_MEDICAID_ADULT %||% TRUE,
    MED_CHILD = inputs$APPLY_MEDICAID_CHILD %||% TRUE,
    ACA       = inputs$APPLY_ACA         %||% TRUE,
    SEC8      = inputs$APPLY_SECTION8    %||% TRUE,
    LIHEAP    = inputs$APPLY_LIHEAP      %||% TRUE,  # we may turn this off for non-CT below
    SNAP      = inputs$APPLY_SNAP        %||% TRUE,
    SLP       = inputs$APPLY_SLP         %||% TRUE,
    WIC       = inputs$APPLY_WIC         %||% TRUE,
    RAP       = inputs$APPLY_RAP         %||% FALSE,
    FRSP      = inputs$APPLY_FRSP        %||% FALSE,
    EITC      = inputs$APPLY_EITC        %||% TRUE,
    CTC       = inputs$APPLY_CTC         %||% TRUE,
    CDCTC     = inputs$APPLY_CDCTC       %||% TRUE
  )
  
  # LIHEAP only for CT
  is_ct <- any(na.omit(df$stateAbbrev) == "CT")
  flags$LIHEAP <- flags$LIHEAP && is_ct
  
  # Benefits (order matters)
  df <- BenefitsCalculator.OtherBenefits(df, flags$TANF, flags$SSI, flags$SSDI)
  df <- BenefitsCalculator.Childcare(df, flags$CHILDCARE, flags$HEADSTART,
                                     flags$PREK, flags$CCDF, flags$FATES)
  df <- BenefitsCalculator.Healthcare(df, flags$HEALTH,
                                      flags$MED_ADULT, flags$MED_CHILD, flags$ACA)
  
  # Food & Housing with LIHEAP-safe fallback
  liheap_log <- file.path(out_dir, "liheap_fallback.log")
  df <- safe_FoodAndHousing(df, flags, log_file = liheap_log)
  
  # Taxes & tax credits
  df <- BenefitsCalculator.TaxesandTaxCredits(df, flags$EITC, flags$CTC, flags$CDCTC)
  
  # Derived variables
  df <- function.createVars(df)
  
  data.table::as.data.table(df)[, ..cols]
}

# Safe wrapper
safe_run <- function(hh_id) {
  if (!exists(".__INIT_DONE__", envir = .GlobalEnv)) {
    init_env(getwd())
  }
  tryCatch(
    run_one(hh_id),
    error = function(e) {
      # Log to file (silent failure)
      try(
        write(
          sprintf("HH %s error: %s", hh_id, e$message),
          file = file.path(out_dir, "error_households.log"),
          append = TRUE
        ),
        silent = TRUE
      )
      NULL
    }
  )
}

# 4) Chunk runner (for one Slurm job / one future)

chunk_runner <- function(ids, dir_out = out_dir) {
  force(dir_out)  # avoid lazy evaluation issues on workers
  if (!dir.exists(dir_out)) {
    dir.create(dir_out, recursive = TRUE, showWarnings = FALSE)
  }
  
  out_list <- lapply(ids, function(x) suppressWarnings(safe_run(x)))
  ok       <- Filter(Negate(is.null), out_list)
  if (length(ok) == 0L) {
    return(invisible(data.table::data.table()))
  }
  
  dt <- data.table::rbindlist(ok, use.names = TRUE, fill = TRUE)
  fname    <- sprintf("household_%s_%s.csv", ids[1], ids[length(ids)])
  out_file <- file.path(dir_out, fname)
  data.table::fwrite(dt, out_file)
  invisible(dt)
}

# 5) Main orchestration function

run_distributed_benefits <- function(root_dir = getwd(),
                                     cfg_dir  = file.path(root_dir, "configs"),
                                     out_dir_ = file.path(root_dir, "output"),
                                     job_size = 50L,
                                     use_slurm = TRUE) {
  
  # Root + output dirs
  setwd(root_dir)
  assign("out_dir", out_dir_, envir = .GlobalEnv)
  if (!dir.exists(out_dir_)) {
    dir.create(out_dir_, recursive = TRUE, showWarnings = FALSE)
  }
  
  # Output columns we care about
  cols <- c(
    "household_id", "ruleYear", "stateFIPS", "stateName", "stateAbbrev",
    "countyortownName", "famsize", "numadults", "numkids",
    paste0("agePerson", 1:12),
    "empl_healthcare", "income", "assets.cash",
    "exp.childcare", "exp.food", "exp.rentormortgage",
    "exp.healthcare", "exp.utilities", "exp.misc", "exp.transportation",
    "netexp.childcare", "netexp.food", "netexp.rentormortgage",
    "netexp.healthcare", "netexp.utilities",
    "value.snap", "value.schoolmeals", "value.section8", "value.liheap",
    "value.medicaid.adult", "value.medicaid.child", "value.aca",
    "value.employerhealthcare",
    "value.CCDF", "value.HeadStart", "value.PreK",
    "value.cdctc.fed", "value.cdctc.state",
    "value.ctc.fed", "value.ctc.state",
    "value.eitc.fed", "value.eitc.state",
    "value.eitc", "value.ctc", "value.cdctc",
    "value.ssdi", "value.ssi", "value.tanf",
    "AfterTaxIncome", "NetResources",
    "tax.income.fed", "tax.income.state"
  )
  assign("cols", cols, envir = .GlobalEnv)
  
  # 5.1 Read YAML configs
  config_files <- list.files(cfg_dir, "\\.yml$", full.names = TRUE)
  if (length(config_files) == 0L) {
    stop("No YAML config files found in: ", cfg_dir)
  }
  
  hh_ids    <- tools::file_path_sans_ext(basename(config_files))
  yaml_list <- setNames(lapply(config_files, yaml::read_yaml), hh_ids)
  assign("yaml_list", yaml_list, envir = .GlobalEnv)
  
  # 5.2 Optional: export flattened YAML inputs for QA
  param_rows <- lapply(names(yaml_list), function(id) {
    flat <- flatten_list(yaml_list[[id]])
    
    id_candidates <- c(
      "H_IDNUM", "h_idnum", "household_id",
      "cps_hh_id", "cps_id", "cps_household_id"
    )
    H_IDNUM_val <- NA_character_
    for (cand in id_candidates) {
      if (!is.null(flat[[cand]])) {
        H_IDNUM_val <- as_chr_id(flat[[cand]])
        break
      }
    }
    flat[["household_id"]] <- NULL
    flat$household_id <- id
    flat$H_IDNUM      <- H_IDNUM_val
    
    as.data.table(flat)
  })
  
  yaml_params_dt <- rbindlist(param_rows, fill = TRUE)
  front_cols     <- intersect(c("household_id", "H_IDNUM"), names(yaml_params_dt))
  setcolorder(yaml_params_dt, c(front_cols, setdiff(names(yaml_params_dt), front_cols)))
  data.table::fwrite(yaml_params_dt, file.path(out_dir_, "yaml_inputs.csv"))
  
  # 5.3 Initialize environment once on the master
  init_env(root_dir)
  
  # 5.4 Choose plan: Slurm or local multisession
  if (use_slurm) {
    plan(
      batchtools_slurm,
      template  = "slurm.tmpl",
      resources = list(ncpus = 16, mem = "180G", time = "08:00:00")
    )
  } else {
    plan(multisession, workers = parallel::detectCores())
  }
  
  # 5.5 Build chunks & submit
  full_chks <- split(hh_ids, ceiling(seq_along(hh_ids) / job_size))
  
  invisible(
    future_lapply(
      X               = full_chks,
      FUN             = chunk_runner,
      future.packages = c("yaml", "data.table", "compiler"),
      future.globals  = c(
        "chunk_runner", "safe_run", "run_one", "safe_FoodAndHousing",
        "init_env", "yaml_list", "cols", "%||%", "scalarize", "out_dir"
      )
    )
  )
  
  # 5.6 Combine all outputs (if present)
  files <- list.files(
    path    = out_dir_,
    pattern = "^household_household_[0-9]+_household_[0-9]+\\.csv$",
    full.names = TRUE
  )
  
  if (length(files) == 0L) {
    warning("No per-chunk output files found to combine in: ", out_dir_)
    return(invisible(data.table::data.table()))
  }
  
  combined_dt <- rbindlist(
    lapply(files, fread, showProgress = TRUE),
    use.names = TRUE,
    fill      = TRUE
  )
  
  output_file <- file.path(out_dir_, "all_households.csv")
  fwrite(combined_dt, output_file)
  
  invisible(combined_dt)
}

# 6) Example usage when run as a script (Rscript 03_run_distributed.R)

if (sys.nframe() == 0) {
  # In the actual project, root_dir would be the directory containing:
  #   - configs/
  #   - prd_parameters/
  #   - libraries.R, functions/*.R
  # Here we show only how the function would be called.
  run_distributed_benefits(
    root_dir  = getwd(),
    cfg_dir   = file.path(getwd(), "configs"),
    out_dir_  = file.path(getwd(), "output"),
    job_size  = 50L,
    use_slurm = TRUE  # set FALSE to run locally via multisession
  )
}
