# Task 1: Calculate genotype concordance 

Steps:
  1. MEGA data needs to be lifted over to hg38 first! 
  2. **concordance_prepfiles.ipynb**: Identify GRIDs and autosomal variants (CHR:POS) genotyped in the MEGA dataset (biovu_megaex_20231001_v2_plink_hg38)
  3. **VUMC_CalculateGenotypeConcordance WDL**: Extract the GRIDs and variants genotyped in MEGA. SNPs are mapped to hg38 and SNP IDs renamed as "CHR:POS:REF:ALT". Multi-allelic variants, rare (MAF<1%), and variants with high missingness (>1%) were excluded. Outputs are PLINK files for both subsets with calculated MAF and missingness.
  4. **VUMC_CalculateGenotypeConcordance WDL**: Identify genotyping differences using PLINK's pgen-diff. Output is pgen-diff file with all discordant reads.
  5. **concordance_summary.Rmd**: Calculate variant-level and person-level discordance rates across all SNPs that are common (MAF>1%), high quality (Missingness<1%), and non-palindromic. 

## Required input data 

- **chromosomes (Array[String])**: chromosomes to process: this.agd35k_bed_alls.chromosome
- **agd_pgen_files (Array[File])**: AGD pgen files: this.agd35k_bed_alls.pgen_pgen
- **agd_psam_files s  (Array[File])**: AGD psam files: this.agd35k_bed_alls.pgen_psam
- **agd_pvar_files  (Array[File])**: AGD pvar files: this.agd35k_bed_alls.pgen_pvar
- **agd_overlap_person_extract_file (File)**: GRIDs with joint MEGA genotyping-AGD sequencing, identified in _concordance_prepfiles.ipynb_

- **mega_bed_file (File)**: "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/uploads/megaex_000_a_best_call_v2_hg38/megaex_BestOfMultipleCalls_v2_hg38.bed"
- **mega_bim_file (File)**: "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/uploads/megaex_000_a_best_call_v2_hg38/megaex_BestOfMultipleCalls_v2_hg38.bim"
- **mega_fam_file (File)**: "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/uploads/megaex_000_a_best_call_v2_hg38/megaex_BestOfMultipleCalls_v2_hg38.fam"
- **mega_overlap_person_extract_file (File)**: GRIDs with joint MEGA genotyping-AGD sequencing, identified in _concordance_prepfiles.ipynb_

- **overlap_variants_extract_file (File)**: variants genotyped in both MEGA & AGD, identified in _concordance_prepfiles.ipynb_
- **fasta_file**: define as hg38 in the workspace
- **ica_to_grid_map (File)**: AGD ID map file: "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/20240303_agd35k_ica_primary_eligible.txt"

- **update_ids_agd (String)**: "AGD_MEGAsubset" - prefix for overlapping subset of AGD sequencing
- **update_ids_mega (String)**: "MEGA_AGDsubset" - prefix for overlapping subset of MEGA genotyping

- **target_gcp_folder (String)**:  GCP folder to which the output files will be copied: "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/concordance/"



# Task 2: Assess concorance between EHR-recorded gender and genetic sex

Steps:
  1. _concordance_prepfiles.ipynb_: Identify GRIDs for discordance analysis and pull EHR-recorded gender as recorded in EPIC 
  1. _VUMC_PLINK_SexCheck_Array WDL_: Estimate the genetic sex using PLINK. Analysis is stratitifed by ancestry. Variants are limited to common variants outside of the pseudoautosomal region.
  2. _concordance_summary.Rmd_: Compare genetic sex calls from PLINK and DRAGEN (WGS)

## Required input data 

- **pgen_files (Array[File])**: "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/uploads/megaex_000_a_best_call_v2_hg38/megaex_BestOfMultipleCalls_v2_hg38.bed"
- **pvar_files (Array[File])**: "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/uploads/megaex_000_a_best_call_v2_hg38/megaex_BestOfMultipleCalls_v2_hg38.bim"
- **psam_files (Array[File])**: "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/uploads/megaex_000_a_best_call_v2_hg38/megaex_BestOfMultipleCalls_v2_hg38.fam"

- **person_extract_file (File)**: GRIDs with AGD sequencing, identified in _concordance_prepfiles.ipynb_
- **update_sex (File)**: Change EPIC_GENDER in .fam file

- **target_prefix (String)**: "AGD35k"
- **target_gcp_folder (String)**:  GCP folder to which the output files will be copied: "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/concordance/"
