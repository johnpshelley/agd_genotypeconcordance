## Calculate genotype concordance 

Steps:
1. 
2.
3.
4.

# Description of input data 

Start with a set of AGD chromosomes in a data table, such that the following columns are arrays of strings or of file paths: chromosomes, pgen, psam and pvar files. 
Include only the autosomes. 

# Required input data 

- id_map_file (File): AGD ID map file: "gs://working-set-redeposit/ica-agd/cohort_001/20240303_agd35k_ica_primary_eligible.txt"
- Chromosomes (Array[String]): chromosomes to process: this.agd35k_bed_alls.chromosome
- source_pgen_files (Array[File]): AGD pgen files: this.agd35k_bed_alls.pgen_pgen
- source_psam_files s  (Array[File]): AGD psam files: this.agd35k_bed_alls.pgen_psam
- source_pvar_files  (Array[File]): AGD pvar files: this.agd35k_bed_alls.pgen_pvar

- target_prefix (String): 20240827_AGD35K_ancestry - prefix for all the output files 

- target_gcp_folder (String):  GCP folder to which the output files will be copied: "gs://fc-secure-540f27be-97ea-4ffd-adb7-c195458eb278/AGD_ancestry_pipeline/"

# Required input choices that have defaults: 

- external_spike_in (Boolean): true
- run_pca (Boolean): true
- run_scope (Boolean): true
- scope_supervised (Boolean): true


## Compare sex discordance results (PLINK vs. DRAGEN)  
