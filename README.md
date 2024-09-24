# Calculate genotype concordance 

Steps:
  1. _JS_GetMEGAVariants.ipynb_: Identify GRIDs and autosomal variants (CHR:POS) genotyped in MEGA dataset (biovu_megaex_20231001_v2_plink_hg38)
  2. _VUMC_CalculateGenotypeConcordance WDL_: Extract from AGD the GRIDs and variants genotyped in MEGA. SNPs are mapped to hg38 and SNP IDs renamed according to CHR:POS:REF:ALT. Multi-allelic variants were also excluded. Missingness and allele frequency for each variant is also calculated.
  3. _VUMC_CalculateGenotypeConcordance WDL_: Identify genotyping differences using PLINK's pgen-diff. 
  4. _VUMC_CalculateGenotypeConcordance WDL_: Copy files to concordance folder within WGS-Flfagship QAQC workspace.
  5. _JS_CalculateDiscordance.Rmd_: Calculate variant-level and person-level discordance rates across all SNPs that are common (MAF>1%), high quality (Missingness<1%), and non-palindromic. 

## Description of input data 

Define people and variants with joint genotyping & sequencing. Using these subsets of people and variants, subset MEGA genotyping and AGD sequencing datasets for pgen-diff comparison.

## Required input data 

- chromosomes (Array[String]): chromosomes to process: this.agd35k_bed_alls.chromosome
- agd_pgen_files (Array[File]): AGD pgen files: this.agd35k_bed_alls.pgen_pgen
- agd_psam_files s  (Array[File]): AGD psam files: this.agd35k_bed_alls.pgen_psam
- agd_pvar_files  (Array[File]): AGD pvar files: this.agd35k_bed_alls.pgen_pvar
- agd_overlap_person_extract_file (File): GRIDs with joint MEGA genotyping-AGD sequencing, identified in _JS_GetMEGAVariants.ipynb_

- mega_bed_file (File): "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/uploads/megaex_000_a_best_call_v2_hg38/megaex_BestOfMultipleCalls_v2_hg38.bed"
- mega_bim_file (File): "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/uploads/megaex_000_a_best_call_v2_hg38/megaex_BestOfMultipleCalls_v2_hg38.bim"
- mega_fam_file (File): "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/uploads/megaex_000_a_best_call_v2_hg38/megaex_BestOfMultipleCalls_v2_hg38.fam"
- mega_overlap_person_extract_file (File): GRIDs with joint MEGA genotyping-AGD sequencing, identified in _JS_GetMEGAVariants.ipynb_

- overlap_variants_extract_file (File): variants genotyped in both MEGA & AGD, identified in _JS_GetMEGAVariants.ipynb_
- fasta_file: define as hg38 in the workspace
- ica_to_grid_map (File): AGD ID map file: "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/20240303_agd35k_ica_primary_eligible.txt"

- update_ids_agd (String): "AGD_MEGAsubset" - prefix for overlapping subset of AGD sequencing
- update_ids_mega (String): "MEGA_AGDsubset" - prefix for overlapping subset of MEGA genotyping

- target_gcp_folder (String):  GCP folder to which the output files will be copied: "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/concordance/"



# Compare sex discordance results (PLINK vs. DRAGEN)  

Steps:
  1. _JS_GetMEGAVariants.ipynb_: Pull gender as recorded in EPIC for each participant with both genotyping and sequencing.
  1. _VUMC_PLINK_SexCheck WDL_: Using the MEGA genotyping subset, estimate the genetic sex using PLINK. 
  2. _JS_CalculateDiscordance.Rmd_: Compare genetic sex calls from PLINK (genotyping) and DRAGEN (WGS)

## Required input data 

- bed_file (File): "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/uploads/megaex_000_a_best_call_v2_hg38/megaex_BestOfMultipleCalls_v2_hg38.bed"
- bim_file (File): "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/uploads/megaex_000_a_best_call_v2_hg38/megaex_BestOfMultipleCalls_v2_hg38.bim"
- fam_file (File): "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/uploads/megaex_000_a_best_call_v2_hg38/megaex_BestOfMultipleCalls_v2_hg38.fam"

- person_extract_file (File): GRIDs with joint MEGA genotyping-AGD sequencing, identified in _JS_GetMEGAVariants.ipynb_
- update_sex (File): Change EPIC_GENDER in .fam file

- target_prefix (String): "MEGA_20231001_v2_hg38" - prefix for overlapping subset of AGD sequencing
- target_gcp_folder (String):  GCP folder to which the output files will be copied: "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/concordance/"
