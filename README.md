# US Benefit Cliffs & Marginal Tax Rate Simulation

**Author:** Yuexin (Joy) Wang  
**Context:** Predoc Research Assistant – Coding Sample

## Overview

This repository is a **code-only snapshot** of a research pipeline I worked on to estimate effective marginal tax rates and benefit cliffs in the United States. The original project integrates CPS ASEC microdata with detailed state and federal policy rules (SNAP, TANF, Medicaid, etc.) to simulate household budgets under counterfactual income paths.

Because the underlying microdata and policy files are confidential, this repository contains **code only**. It is meant to show how I structure and implement this type of project, rather than to be fully reproducible.

### What this repo showcases

1. **Data cleaning with messy identifiers** – harmonizing administrative strings and geography (counties, CBSAs, towns).  
2. **Pipeline engineering** – turning flat survey files into structured household configurations (YAML) for a microsimulation engine.  
3. **High-performance computing (HPC)** – running large simulations on a Slurm cluster using R’s `future` / `future.batchtools` tools.

---

## Repository structure

```text
.
├── R/
│   ├── 01_impute_geography.R        # Spatial imputation / cleaning for CPS households
│   ├── 02_generate_yaml_configs.R   # Serialize household data into YAML simulation inputs
│   └── 03_run_distributed.R         # Distributed simulation driver (Slurm + future)
├── Rmd/
│   ├── Analysis.Rmd                 # Narrative documentation of the workflow
│   ├── Analysis.pdf                 # Knit report (analysis + code)
│   ├── writing_sample_code.Rmd      # Technical commentary on implementation choices
│   └── Wang_Yuexin_Writing_Sample.pdf   # Knit writing sample
├── python/
│   ├── main.py                      # Python scripting example (aggregation)
│   └── code_sample_predoc.ipynb     # Python notebook example (EDA / visualization)
└── data-raw/                        # (Not included) CPS and crosswalk inputs
````

In the original project, the R scripts read CPS and crosswalk files from `data-raw/`, write household configs to `configs/`, and save simulation outputs under `output/`. Those folders and files are not part of this public repo.

---

## Pipeline detail

### 1. Spatial imputation & data harmonization

**Script:** `R/01_impute_geography.R`

CPS household data do not always contain the county/town detail needed for local tax and benefit calculations. This script builds a cleaned geography layer and imputes missing counties.

* **Integration:** Census county population estimates, `tidycensus::fips_codes`, and CBSA-to-county crosswalks.
* **Cleaning:** Standardizes suffixes such as *County*, *Parish*, *Borough*, and handles Connecticut’s town-based system explicitly.
* **Imputation logic:**

  1. Use CBSA codes to pick the most populous county within each CBSA.
  2. For households with missing geography, assign the most populous county in the state that is *not* already used (to avoid double-counting).
  3. If no candidate remains, fall back to the state’s single largest county.

The output (in the full project) is a CPS household file with `final_GTCO`, `final_state_name`, `final_county_name`, and a human-readable `final_location`.

---

### 2. Simulation configuration (YAML generation)

**Script:** `R/02_generate_yaml_configs.R`

This script prepares households for the simulation engine by turning rows of survey data into per-household YAML configuration files.

* **Reshaping:** Builds a wide person-level table (up to 12 people) with ages, disability, blindness, and SSDI benefit amounts.
* **Feature engineering:**

  * Reorders persons so adults come first (simplifies eligibility logic).
  * Builds household-level flags such as `married` (based on reference persons) and `prev_ssi`.
* **Output:** For each household, writes a YAML file that contains:

  * policy switches (e.g. `APPLY_SNAP`, `APPLY_TAXES`, `APPLY_MEDICAID_ADULT`),
  * household composition,
  * assets and disability-related variables,
  * geography and tenure (own vs rent).

---

### 3. Distributed HPC orchestration

**Script:** `R/03_run_distributed.R`

This script shows how I run the benefit calculator at scale on a Slurm cluster.

* **Environment management:** `init_env()` loads parameter `.rdata` files, sources benefit / expense / tax functions, and JIT-compiles “hot” functions with `compiler::cmpfun()`.
* **Single-household runner:** `run_one()` reads one YAML config, builds the base dataframe, applies ALICE expenses, programs (TANF / SSI / SSDI / childcare / healthcare / food & housing), and finally taxes and derived variables.
* **Fault tolerance:** `safe_run()` wraps each household in `tryCatch` and logs errors so one failing case does not crash the entire job. `safe_FoodAndHousing()` handles LIHEAP errors and retries with LIHEAP disabled.
* **Parallelization:** `run_distributed_benefits()`

  * splits household IDs into chunks (e.g. 50 per job),
  * submits them via `future.batchtools` using a Slurm template,
  * and then combines chunk outputs into a single `all_households.csv`.

---

## Supplementary materials

### R Markdown (`Rmd/`)

* **`Analysis.Rmd` / `Analysis.pdf`** – a narrative walkthrough of the simulation logic and how the R scripts fit together in the broader economic question.
* **`writing_sample_code.Rmd` / `Wang_Yuexin_Writing_Sample.pdf`** – more focused on code design choices, edge-case handling (e.g. Connecticut towns), and performance considerations.

These files are included mainly as *writing + coding* examples; they will not knit end-to-end without the original data.

### Python examples (`python/`)

* **`main.py`** – a small script showing similar aggregation tasks in Python (e.g. collapsing plan-level data to the county level).
* **`code_sample_predoc.ipynb`** – a notebook illustrating how I organize exploratory analysis and visualization in Python.

---

## Reproducibility

This pipeline is **not** intended to be fully reproducible outside the original project environment. It relies on:

1. **Confidential CPS ASEC microdata** with detailed geography and income variables.
2. **Internal parameter files** describing state-specific program rules.
3. **A restricted Slurm cluster at the University of Michigan.**

The code here is provided so that readers can evaluate the logic, organization, and documentation style I use when working on large applied projects.

---

## Contact

**Yuexin (Joy) Wang**
University of Michigan
Email: [yuexinwa@umich.edu](mailto:yuexinwa@umich.edu)


