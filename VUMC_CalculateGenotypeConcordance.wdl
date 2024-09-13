version 1.0

#IMPORTS
## According to this: https://cromwell.readthedocs.io/en/stable/Imports/ we can import raw from github
## so we can make use of the already written WDLs provided by WARP/VUMC Biostatistics

import "https://raw.githubusercontent.com/shengqh/warp/develop/tasks/vumc_biostatistics/GcpUtils.wdl" as http_GcpUtils
import "https://raw.githubusercontent.com/shengqh/warp/develop/pipelines/vumc_biostatistics/genotype/Utils.wdl" as http_GenotypeUtils
import "https://raw.githubusercontent.com/shengqh/warp/develop/pipelines/vumc_biostatistics/agd/AgdUtils.wdl" as http_AgdUtils
import "https://raw.githubusercontent.com/johnpshelley/agd_genotypeconcordance/main/ExtractSubset.wdl" as ExtractSubset_Array
import "https://raw.githubusercontent.com/johnpshelley/agd_genotypeconcordance/main/ExtractSubset_NoFileArray.wdl" as ExtractSubset_NoFileArray

workflow IdentifyDiscordantVariants {
  input {
    File mega_bed_file
    File mega_bim_file
    File mega_fam_file
    String output_prefix_mega

    Array[File] agd_pgen_files
    Array[File] agd_pvar_files
    Array[File] agd_psam_files
    Array[String] chromosomes
    String output_prefix_agd

    String output_folder
    File overlap_variants_extract_file
    File overlap_person_extract_file

    File update_ids1
    File update_ids2

    File ica_to_grid_map
    File fasta_file
  }

    call ExtractSubset_NoFileArray.ExtractSubset as ExtractSubset_MEGA {
        input:
        bed_file = mega_bed_file,
        bim_file = mega_bim_file,
        fam_file = mega_fam_file,
        id_map_file = ica_to_grid_map,
        fasta_file = fasta_file,
        variants_extract_file = overlap_variants_extract_file,
        person_extract_file = overlap_person_extract_file,
        target_prefix = output_prefix_mega,
        target_gcp_folder = output_folder
  }

      if(defined(target_gcp_folder)){
    call http_GcpUtils.MoveOrCopyThreeFiles as CopyFiles_MEGA {
      input:
        source_file1 = ExtractSubset_MEGA.output_pgen_file,
        source_file2 = ExtractSubset_MEGA.output_pvar_file,
        source_file3 = ExtractSubset_MEGA.output_psam_file,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

    call ExtractSubset_Array.ExtractSubset as ExtractSubset_AGD {
        input:
        pgen_files = agd_pgen_files,
        pvar_files = agd_pvar_files,
        psam_files = agd_psam_files,
        id_map_file = ica_to_grid_map,
        fasta_file = fasta_file,
        variants_extract_file = overlap_variants_extract_file,
        person_extract_file = overlap_person_extract_file,
        target_prefix = output_prefix_agd,
        target_gcp_folder = output_folder
    }
      
      if(defined(target_gcp_folder)){
    call http_GcpUtils.MoveOrCopyThreeFiles as CopyFiles_AGD {
      input:
        source_file1 = ExtractSubset_AGD.output_freq_file,
        source_file2 = ExtractSubset_AGD.output_geno_miss_file,
        source_file3 = ExtractSubset_AGD.output_person_miss_file,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

    call PLINK_pgendiff as PLINK_pgendiff {
      input:
        pgen_file_1 = ExtractSubset_AGD.output_pgen_file,
        pvar_file_1 = ExtractSubset_AGD.output_pvar_file,
        psam_file_1 = ExtractSubset_AGD.output_psam_file,
        pgen_file_2 = ExtractSubset_MEGA.output_pgen_file,
        pvar_file_2 = ExtractSubset_MEGA.output_pvar_file,
        psam_file_2 = ExtractSubset_MEGA.output_psam_file,
        update_ids1 = update_ids1,
        update_ids2 = update_ids2
    }
  
    if(defined(target_gcp_folder)){
    call http_GcpUtils.MoveOrCopyTwoFiles as CopyFiles_pgendiff {
      input:
        source_file1 = PLINK_pgendiff.output_plink_pgendiff,
        source_file2 = PLINK_pgendiff.output_plink_log,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File mega_maf = select_first([CopyFiles_MEGA.output_file1, MergePgenFiles_MOD.output_freq_file])
    File mega_miss_geno = select_first([CopyFiles_MEGA.output_file2, MergePgenFiles_MOD.output_geno_miss_file])
    File mega_miss_person = select_first([CopyFiles_MEGA.output_file3, MergePgenFiles_MOD.output_person_miss_file])
    
    File agd_maf  = select_first([CopyFiles_AGD.output_file1, MergePgenFiles_MOD.output_freq_file])
    File agd_miss_geno  = select_first([CopyFiles_AGD.output_file2, MergePgenFiles_MOD.output_geno_miss_file])
    File agd_miss_person  = select_first([CopyFiles_AGD.output_file3, MergePgenFiles_MOD.output_person_miss_file])

    File output_pgen_file = select_first([CopyFiles_pgendiff.output_file1, PLINK_pgendiff.output_plink_pgendiff])
    File output_pvar_file = select_first([CopyFiles_pgendiff.output_file2, PLINK_pgendiff.output_plink_log])
  }
}

### TASK DEFINITIONS

task PLINK_pgendiff{
  input {
    File pgen_file_1
    File pvar_file_1
    File psam_file_1
    
    File update_ids1
    
    File pgen_file_2
    File pvar_file_2
    File psam_file_2
    
    File update_ids2

    Int memory_gb = 20

    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
  }

  Int disk_size = ceil(size([pgen_file_1, psam_file_1, pvar_file_1, pgen_file_2, psam_file_2, pvar_file_2], "GB")  * 3) + 20

  String output_plink_pgendiff = "plink2.log"
  String output_plink_log = "plink2.pdiff"

## FIDs need to be changed to GRIDs. Remove duplicate variants in MEGA.

  command {
  
      plink2 \
      --pgen ~{pgen_file_1} \
      --pvar ~{pvar_file_1} \
      --psam ~{psam_file_1} \
      --update-ids ~{update_ids1} \
      --rm-dup force-first \
      --make-pgen \
      --out intermediate1
      
            plink2 \
      --pgen ~{pgen_file_2} \
      --pvar ~{pvar_file_2} \
      --psam ~{psam_file_2} \
      --update-ids ~{update_ids2} \
      --rm-dup force-first \
      --make-pgen \
      --out intermediate2
      
      plink2 \
      --pfile intermediate1 \
      --pgen-diff intermediate2
  }

  runtime {
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }

  output {
    File output_plink_pgendiff = output_plink_pgendiff
    File output_plink_log = output_plink_log
  }

}