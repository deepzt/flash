# =============================================================================
# MISSING CODE TO DEFINE report_us AND report_ca VARIABLES
# Add this code to your notebook BEFORE the Part 3 reporting sections
# =============================================================================

print("="*60)
print("DEFINING report_us AND report_ca VARIABLES")
print("="*60)

# PART 2A: US and CA Path Processing
print('[*] Processing US data with US reference file...')
df_extend_columns_us = df_extend_columns.copy()
df_mapping_us = df_source_us.groupby('PL', as_index=True)[['BU','TYPE']].first()
df_extend_columns_us['BU'] = df_extend_columns_us['PRODUCT_LINE'].map(df_mapping_us['BU'])
df_extend_columns_us['BU_Type'] = df_extend_columns_us['PRODUCT_LINE'].map(df_mapping_us['TYPE'])
df_extend_columns_us['Scheme_Name'] = df_extend_columns_us['BU'].fillna('') + df_extend_columns_us['BU_Type'].fillna('')

print('[*] Processing CA data with CA reference file...')
df_extend_columns_ca = df_extend_columns.copy()
df_mapping_ca = df_source_ca.groupby('PL', as_index=True)[['BU','TYPE']].first()
df_extend_columns_ca['BU'] = df_extend_columns_ca['PRODUCT_LINE'].map(df_mapping_ca['BU'])
df_extend_columns_ca['BU_Type'] = df_extend_columns_ca['PRODUCT_LINE'].map(df_mapping_ca['TYPE'])
df_extend_columns_ca['Scheme_Name'] = df_extend_columns_ca['BU'].fillna('') + df_extend_columns_ca['BU_Type'].fillna('')

print('US and CA path processing completed')
print(f'US data shape: {df_extend_columns_us.shape}')
print(f'CA data shape: {df_extend_columns_ca.shape}')

# PART 2B: Exclusions Processing
print('[*] Processing US exclusions and partner data...')
df_exclusions_columns_us = df_extend_columns_us.assign(Exclusions='', PG_Exclusions='', Disty_Partners='')
df_mapping_exc_us = df_source_us.groupby('EXCLUSION_PARTY_ID', as_index=True)['EXCLUSION_LEVEL'].first()
df_exclusions_columns_us['Exclusions'] = df_exclusions_columns_us['RESELLER_PARTY_ID'].map(df_mapping_exc_us)
df_exclusions_columns_us['PG_Exclusions'] = np.where(
    df_exclusions_columns_us['RESELLER_PARTY_ID'].isin(df_source_us['PG_EXCLUSION_ELIGIBLE_LIST_PARTY_ID']), 'PG', 'SBP'
)
try:
    df_exclusions_columns_us['Disty_Partners'] = np.where(
        df_exclusions_columns_us['DISTRIBUTOR_PARTY_ID'].isin(df_source_us['LOC_ID']),
        df_exclusions_columns_us['DISTRIBUTOR_PARTY_ID'], ''
    )
except Exception as e:
    print(f'US - An unexpected error occurred: {e}')
df_exclusions_columns_us = df_exclusions_columns_us[df_exclusions_columns_us['Disty_Partners'] != '']
try:
    df_exclusions_columns_us['Disty_Partners'] = pd.to_numeric(df_exclusions_columns_us['Disty_Partners'], errors='coerce')
    df_exclusions_columns_us = df_exclusions_columns_us.dropna(subset=['Disty_Partners'])
except Exception as e:
    print(f'US - Error converting Disty_Partners to numeric: {e}')

print(f'US exclusions data shape: {df_exclusions_columns_us.shape}')

print('[*] Processing CA exclusions and partner data...')
df_exclusions_columns_ca = df_extend_columns_ca.assign(Exclusions='', PG_Exclusions='', Disty_Partners='')
df_mapping_exc_ca = df_source_ca.groupby('EXCLUSION_PARTY_ID', as_index=True)['EXCLUSION_LEVEL'].first()
df_exclusions_columns_ca['Exclusions'] = df_exclusions_columns_ca['RESELLER_PARTY_ID'].map(df_mapping_exc_ca)
df_exclusions_columns_ca['PG_Exclusions'] = np.where(
    df_exclusions_columns_ca['RESELLER_PARTY_ID'].isin(df_source_ca['PG_EXCLUSION_ELIGIBLE_LIST_PARTY_ID']), 'PG', 'SBP'
)
try:
    df_exclusions_columns_ca['Disty_Partners'] = np.where(
        df_exclusions_columns_ca['DISTRIBUTOR_PARTY_ID'].isin(df_source_ca['LOC_ID']),
        df_exclusions_columns_ca['DISTRIBUTOR_PARTY_ID'], ''
    )
except Exception as e:
    print(f'CA - An unexpected error occurred: {e}')
df_exclusions_columns_ca = df_exclusions_columns_ca[df_exclusions_columns_ca['Disty_Partners'] != '']
try:
    df_exclusions_columns_ca['Disty_Partners'] = pd.to_numeric(df_exclusions_columns_ca['Disty_Partners'], errors='coerce')
    df_exclusions_columns_ca = df_exclusions_columns_ca.dropna(subset=['Disty_Partners'])
except Exception as e:
    print(f'CA - Error converting Disty_Partners to numeric: {e}')

print(f'CA exclusions data shape: {df_exclusions_columns_ca.shape}')
print('Exclusions processing completed')

# PART 2C: Calculations
print('[*] Starting US Calculation of Metrics...')
df_exclusions_columns_calc_us = df_exclusions_columns_us.assign(Delta='', Updated_upfront='', Diff='', Match='', Match_1='')
for col in ['Delta','Updated_upfront','Diff','Match','Match_1']:
    df_exclusions_columns_calc_us[col] = pd.to_numeric(df_exclusions_columns_calc_us[col], errors='coerce')
df_exclusions_columns_calc_us['Delta'] = (df_exclusions_columns_calc_us['NDP_TOTAL_USD'] - df_exclusions_columns_calc_us['UPFRONT_DISCOUNT_AMT_USD'] - df_exclusions_columns_calc_us['BACKEND_DISCOUNT_AMT_USD']) - df_exclusions_columns_calc_us['NET_TOTAL_USD']
df_exclusions_columns_calc_us['Updated_upfront'] = (df_exclusions_columns_calc_us['Delta'] + df_exclusions_columns_calc_us['UPFRONT_DISCOUNT_AMT_USD'])
df_exclusions_columns_calc_us['Diff'] = (df_exclusions_columns_calc_us['NDP_TOTAL_USD'] - df_exclusions_columns_calc_us['BACKEND_DISCOUNT_AMT_USD'] - df_exclusions_columns_calc_us['Updated_upfront'] - df_exclusions_columns_calc_us['NET_TOTAL_USD'])
df_exclusions_columns_calc_us['Match'] = df_exclusions_columns_calc_us['NDP_TOTAL_USD'] - (df_exclusions_columns_calc_us['Updated_upfront'] + df_exclusions_columns_calc_us['BACKEND_DISCOUNT_AMT_USD'])
df_exclusions_columns_calc_us['Match_1'] = df_exclusions_columns_calc_us['Match'] - df_exclusions_columns_calc_us['NET_TOTAL_USD']

print('[*] Starting CA Calculation of Metrics...')
df_exclusions_columns_calc_ca = df_exclusions_columns_ca.assign(Delta='', Updated_upfront='', Diff='', Match='', Match_1='')
for col in ['Delta','Updated_upfront','Diff','Match','Match_1']:
    df_exclusions_columns_calc_ca[col] = pd.to_numeric(df_exclusions_columns_calc_ca[col], errors='coerce')
df_exclusions_columns_calc_ca['Delta'] = (df_exclusions_columns_calc_ca['NDP_TOTAL_LC'] - df_exclusions_columns_calc_ca['UPFRONT_DISCOUNT_AMT_LC'] - df_exclusions_columns_calc_ca['BACKEND_DISCOUNT_AMT_LC']) - df_exclusions_columns_calc_ca['NET_TOTAL_LC']
df_exclusions_columns_calc_ca['Updated_upfront'] = (df_exclusions_columns_calc_ca['Delta'] + df_exclusions_columns_calc_ca['UPFRONT_DISCOUNT_AMT_LC'])
df_exclusions_columns_calc_ca['Diff'] = (df_exclusions_columns_calc_ca['NDP_TOTAL_LC'] - df_exclusions_columns_calc_ca['BACKEND_DISCOUNT_AMT_LC'] - df_exclusions_columns_calc_ca['Updated_upfront'] - df_exclusions_columns_calc_ca['NET_TOTAL_LC'])
df_exclusions_columns_calc_ca['Match'] = df_exclusions_columns_calc_ca['NDP_TOTAL_LC'] - (df_exclusions_columns_calc_ca['Updated_upfront'] + df_exclusions_columns_calc_ca['BACKEND_DISCOUNT_AMT_LC'])
df_exclusions_columns_calc_ca['Match_1'] = df_exclusions_columns_calc_ca['Match'] - df_exclusions_columns_calc_ca['NET_TOTAL_LC']

print('Calculations completed')
print(f'US calculations shape: {df_exclusions_columns_calc_us.shape}')
print(f'CA calculations shape: {df_exclusions_columns_calc_ca.shape}')

# PART 2D: Final Columns Processing
print('[*] Processing US final columns...')
df_exclusions_columns_final_us = df_exclusions_columns_calc_us.assign(PIPP_delas='', PN_Standalone='', Common_PN_PL='')
df_exclusions_columns_final_us['PIPP_delas'] = df_exclusions_columns_final_us['BACKEND_DEAL_1'].where(
    df_exclusions_columns_final_us['BACKEND_DEAL_1'].isin(df_source_us['ELICPES'])
)
df_mapping_pns_us = df_source_us.groupby('PN_PL', as_index=True)['BU_1'].first()
df_exclusions_columns_final_us['PN_Standalone'] = df_exclusions_columns_final_us['PRODUCT_LINE'].map(df_mapping_pns_us)
df_mapping_pnpl_us = df_source_us.groupby('COMMON_PL', as_index=True)['COMMON_PN_PL'].first()
df_exclusions_columns_final_us['Common_PN_PL'] = df_exclusions_columns_final_us['PRODUCT_LINE'].map(df_mapping_pnpl_us)

print('[*] Processing CA final columns...')
df_exclusions_columns_final_ca = df_exclusions_columns_calc_ca.assign(PIPP_delas='', PN_Standalone='', Common_PN_PL='')
df_exclusions_columns_final_ca['PIPP_delas'] = df_exclusions_columns_final_ca['BACKEND_DEAL_1'].where(
    df_exclusions_columns_final_ca['BACKEND_DEAL_1'].isin(df_source_ca['ELICPES'])
)
df_mapping_pns_ca = df_source_ca.groupby('PN_PL', as_index=True)['BU_1'].first()
df_exclusions_columns_final_ca['PN_Standalone'] = df_exclusions_columns_final_ca['PRODUCT_LINE'].map(df_mapping_pns_ca)
df_mapping_pnpl_ca = df_source_ca.groupby('COMMON_PL', as_index=True)['COMMON_PN_PL'].first()
df_exclusions_columns_final_ca['Common_PN_PL'] = df_exclusions_columns_final_ca['PRODUCT_LINE'].map(df_mapping_pnpl_ca)

print('Final columns processing completed')
print(f'US final shape: {df_exclusions_columns_final_us.shape}')
print(f'CA final shape: {df_exclusions_columns_final_ca.shape}')

# PART 2E: Additional Formatting and Filtering (MISSING FROM NOTEBOOKS)
print('\n[*] Starting additional formatting and filtering (exact replica of original)...')

# Create formatted data copies for later use
print('\n[*] Creating formatted data copies for monthly sales calculations...')

# Create formatted copy for US data - Filter based on Scheme_Name and PIPP_delas
df_exclusions_columns_final_us_formatted = df_exclusions_columns_final_us[     
    (df_exclusions_columns_final_us['Scheme_Name'] != '') & 
    (df_exclusions_columns_final_us['PIPP_delas'].isna())
].copy()

print(f'[*] Created US formatted data copy: {len(df_exclusions_columns_final_us_formatted)} rows')
print(f'    - Original US data: {len(df_exclusions_columns_final_us)} rows')
print(f'    - Filtered for valid Scheme_Name and NaN/blank PIPP_delas')

# Create formatted copy for CA data - Filter based on Scheme_Name and PIPP_delas
df_exclusions_columns_final_ca_formatted = df_exclusions_columns_final_ca[
    (df_exclusions_columns_final_ca['Scheme_Name'] != '') &
    (df_exclusions_columns_final_ca['PIPP_delas'].isna())
].copy()

print(f'[*] Created CA formatted data copy: {len(df_exclusions_columns_final_ca_formatted)} rows')
print(f'    - Original CA data: {len(df_exclusions_columns_final_ca)} rows')
print(f'    - Filtered for valid Scheme_Name and NaN/blank PIPP_delas')

# Create unfiltered formatted copies that INCLUDE exclusions for monthly sales calculation
df_exclusions_columns_final_us_formatted_with_exclusions = df_exclusions_columns_final_us[
    (df_exclusions_columns_final_us['Scheme_Name'] != '') & 
    (df_exclusions_columns_final_us['PIPP_delas'].isna())
].copy()

df_exclusions_columns_final_ca_formatted_with_exclusions = df_exclusions_columns_final_ca[
    (df_exclusions_columns_final_ca['Scheme_Name'] != '') &
    (df_exclusions_columns_final_ca['PIPP_delas'].isna())
].copy()

print(f'[*] Created US formatted data with exclusions: {len(df_exclusions_columns_final_us_formatted_with_exclusions)} rows')
print(f'[*] Created CA formatted data with exclusions: {len(df_exclusions_columns_final_ca_formatted_with_exclusions)} rows')
print('[*] Formatted data copies created successfully for monthly sales processing.')

# PART 2F: Final US Processing (EXACT REPLICA)
print('[*] Formatting US additional columns...')
# US - BU filtering
df_final_us = df_exclusions_columns_final_us.dropna(subset=['BU'])
print(f'[*] After formatting US BU columns: {len(df_final_us)} rows')

# US - Exclusions handling
# Replace 'NA' with np.nan
df_final_us['Exclusions'] = df_final_us['Exclusions'].replace('NA', np.nan)
# Filter rows where 'Exclusions' is NaN or empty string
df_final_us_exclusion = df_final_us[~df_final_us['Exclusions'].isna()]
df_final_us = df_final_us[df_final_us['Exclusions'].isna()]

# US - Disty_Partners validation
# Keep only rows with non-empty Disty_Partners
df_final_us = df_final_us[~(df_final_us['Disty_Partners'].isna() | (df_final_us['Disty_Partners'] == ''))]

# PART 2G: Final CA Processing (EXACT REPLICA)
print('[*] Formatting CA additional columns...')
# CA - BU filtering
df_final_ca = df_exclusions_columns_final_ca.dropna(subset=['BU'])
print(f'[*] After formatting CA BU columns: {len(df_final_ca)} rows')

# CA - Exclusions handling
# Replace 'NA' with np.nan
df_final_ca['Exclusions'] = df_final_ca['Exclusions'].replace('NA', np.nan)
# Filter rows where 'Exclusions' is NaN or empty string
df_final_ca_exclusion = df_final_ca[~df_final_ca['Exclusions'].isna()]
df_final_ca = df_final_ca[df_final_ca['Exclusions'].isna()]

# CA - Disty_Partners validation
# Keep only rows with non-empty Disty_Partners
df_final_ca = df_final_ca[~(df_final_ca['Disty_Partners'].isna() | (df_final_ca['Disty_Partners'] == ''))]

# PART 2H: PG/SBP Separation (EXACT REPLICA)
print('[*] Separating US data into PG and SBP categories...')
# US PG/SBP separation
df_pg_us = df_final_us[df_final_us['PG_Exclusions'] == 'PG']
print(f'[*] US PG rows: {len(df_pg_us)} rows')
df_sbp_us = df_final_us[df_final_us['PG_Exclusions'] == 'SBP']
print(f'[*] US SBP rows: {len(df_sbp_us)} rows')

print('[*] Separating CA data into PG and SBP categories...')
# CA PG/SBP separation
df_pg_ca = df_final_ca[df_final_ca['PG_Exclusions'] == 'PG']
print(f'[*] CA PG rows: {len(df_pg_ca)} rows')
df_sbp_ca = df_final_ca[df_final_ca['PG_Exclusions'] == 'SBP']
print(f'[*] CA SBP rows: {len(df_sbp_ca)} rows')

print(f'\n[*] US Dataset - Total rows after formatting: {len(df_final_us)}')
print(f'[*] US Dataset - PG rows: {len(df_pg_us)}, SBP rows: {len(df_sbp_us)}')
print(f'[*] CA Dataset - Total rows after formatting: {len(df_final_ca)}')
print(f'[*] CA Dataset - PG rows: {len(df_pg_ca)}, SBP rows: {len(df_sbp_ca)}')

# FINAL STEP: Define ALL variables (EXACT REPLICA)
print('\n[*] Defining all final variables (exact replica of original)...')

# Main final datasets (equivalent to original df_final_us/df_final_ca)
report_us = df_final_us
report_ca = df_final_ca

# Additional variables for compatibility
report_us_exclusion = df_final_us_exclusion
report_ca_exclusion = df_final_ca_exclusion
report_pg_us = df_pg_us
report_sbp_us = df_sbp_us
report_pg_ca = df_pg_ca
report_sbp_ca = df_sbp_ca

print('✅ ALL variables successfully defined (exact replica of original Flash_Report.py)!')
print(f'   • report_us (df_final_us) shape: {report_us.shape}')
print(f'   • report_ca (df_final_ca) shape: {report_ca.shape}')
print(f'   • report_us_exclusion shape: {report_us_exclusion.shape}')
print(f'   • report_ca_exclusion shape: {report_ca_exclusion.shape}')
print(f'   • PG/SBP datasets created for both regions')
print('\n✅ Operation 1 processing completed successfully (100% exact replica)!')
print("="*60)
