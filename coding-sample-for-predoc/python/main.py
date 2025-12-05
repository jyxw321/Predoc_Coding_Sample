"""
main.py

Goal:
    Aggregate Medicare Advantage plan-level data to the county level.

    - Read a CSV with plan-level enrollment and penetration
    - Construct indicators for:
        * numberofplans1: plans with > 10 enrollees
        * numberofplans2: plans with penetration > 0.5
    - Aggregate to (state, countyname, countyssa) level
    - Export 'county_enrollment.csv' to ../output/

Note:
    Input data (scp-1205.csv) is not included in this repo.
"""

import pandas as pd
import numpy as np
import time
import os


def main():
    # Start timer to track performance
    start_time = time.time()

    # --- 1. Setup Paths (Robust/Absolute Paths) ---
    # This ensures the code finds the file regardless of where the script is run from
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(script_dir, '../input/scp-1205.csv')
    output_path = os.path.join(script_dir, '../output/county_enrollment.csv')

    print(f"1. Loading data from: {os.path.abspath(input_path)}")

    # --- 2. Load Data ---
    # The raw CSV lacks a standard header or has a malformed one.
    # We manually define columns based on the data structure (including the 'contract' column).
    column_names = [
        'countyname', 'state', 'contract', 'healthplanname',
        'typeofplan', 'countyssa', 'eligibles', 'enrollees',
        'penetration', 'ABrate'
    ]

    try:
        # Load data without a header, manually assigning names
        # 'thousands' parameter handles comma separators in numbers (e.g., "1,000")
        df = pd.read_csv(input_path, header=None, names=column_names, thousands=',')

        # Data Quality Check: Remove the original header row if it exists in the data
        # We check if the first row's 'eligibles' column contains the text "eligibles"
        if str(df['eligibles'].iloc[0]).lower().strip() == 'eligibles':
            print("   Detected header row in data body. Removing it...")
            df = df.iloc[1:].reset_index(drop=True)

    except FileNotFoundError:
        print(f"Error: File not found at {input_path}")
        print("Please ensure 'scp-1205.csv' is in the 'input' folder.")
        return

    # --- 3. Data Cleaning & Pre-processing ---
    print("2. Cleaning and processing variables...")

    # 3.1 Whitespace Removal (CRITICAL STEP)
    # Raw data often contains trailing spaces (e.g., "GU " instead of "GU").
    # We strip whitespace from all string identifiers to ensure filters work.
    string_cols = ['countyname', 'state', 'contract', 'countyssa']
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # 3.2 Handle Missing Numeric Values
    # Instructions: Missing values for eligibles, enrollees, penetration should be 0.
    fill_zero_cols = ['eligibles', 'enrollees', 'penetration']
    for col in fill_zero_cols:
        # Coerce non-numeric errors to NaN, then fill with 0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # 3.3 Exclude Territories & Invalid Rows
    # Instructions: Exclude Puerto Rico (PR) and Guam (GU).
    # Judgment Call: Also excluding VI, AS, MP, and rows with invalid/missing state codes (e.g., "UNDER-11" rows).
    excluded_territories = ['PR', 'GU', 'VI', 'AS', 'MP']

    # Filter Logic:
    # 1. State is NOT in the exclusion list
    # 2. State is NOT 'nan' (removes dirty rows)
    # 3. Countyname is NOT specific suppressed data markers
    initial_rows = len(df)
    mask = (
            ~df['state'].isin(excluded_territories) &
            (df['state'] != 'nan') &
            (df['state'] != '') &
            (df['countyname'] != 'Unusual SCounty Code') &
            (df['countyname'] != 'UNDER-11')
    )
    df = df[mask].copy()
    print(f"   Rows filtered: {initial_rows - len(df)} (Territories and invalid data removed)")

    # 3.4 Create Indicator Variables
    # numberofplans1: Plans with > 10 enrollees
    df['plan_gt_10'] = (df['enrollees'] > 10).astype(int)

    # numberofplans2: Plans with penetration > 0.5
    df['pen_gt_0.5'] = (df['penetration'] > 0.5).astype(int)

    # --- 4. Aggregation ---
    print("3. Aggregating to county level...")

    # Aggregation Logic:
    # - Sum plan counts and total enrollees.
    # - Take MAX of 'eligibles'. 'eligibles' is a county-level total repeated on every plan row.
    #   Summing it would incorrectly inflate the eligible population.
    aggregation_rules = {
        'plan_gt_10': 'sum',
        'pen_gt_0.5': 'sum',
        'eligibles': 'max',  # Judgment Call: Use max to capture the county constant
        'enrollees': 'sum'
    }

    # Group by unique county identifiers
    county_level = df.groupby(['state', 'countyname', 'countyssa'], as_index=False).agg(aggregation_rules)

    # --- 5. Final Calculations & Formatting ---
    print("4. Finalizing dataset...")

    # Rename columns to match requirements
    county_level = county_level.rename(columns={
        'plan_gt_10': 'numberofplans1',
        'pen_gt_0.5': 'numberofplans2',
        'enrollees': 'totalenrollees'
    })

    # Calculate 'totalpenetration'
    # Formula: 100 * (totalenrollees / eligibles)
    # Use np.where to avoid DivisionByZero errors
    county_level['totalpenetration'] = np.where(
        county_level['eligibles'] > 0,
        (county_level['totalenrollees'] / county_level['eligibles']) * 100,
        0
    )

    # Sort by state then county
    county_level = county_level.sort_values(by=['state', 'countyname'])

    # Reorder columns strictly as requested
    final_columns = [
        'countyname', 'state', 'numberofplans1', 'numberofplans2',
        'countyssa', 'eligibles', 'totalenrollees', 'totalpenetration'
    ]
    county_level = county_level[final_columns]

    # --- 6. Export ---
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    county_level.to_csv(output_path, index=False)

    end_time = time.time()
    print(f"Done. Output saved to: {output_path}")
    print(f"Total execution time: {end_time - start_time:.4f} seconds")


if __name__ == "__main__":
    main()
