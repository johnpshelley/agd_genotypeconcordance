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
    File mega_overlap_person_extract_file

    Array[File] agd_pgen_files
    Array[File] agd_pvar_files
    Array[File] agd_psam_files
    Array[String] chromosomes
    String output_prefix_agd
    File agd_overlap_person_extract_file

    String output_folder

    File overlap_variants_extract_file

    File update_ids_mega
    File update_ids_agd

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
        person_extract_file = mega_overlap_person_extract_file,
        target_prefix = output_prefix_mega,
        target_gcp_folder = output_folder
  }

    call ExtractSubset_Array.ExtractSubset as ExtractSubset_AGD {
        input:
        pgen_files = agd_pgen_files,
        pvar_files = agd_pvar_files,
        psam_files = agd_psam_files,
        id_map_file = ica_to_grid_map,
        fasta_file = fasta_file,
        variants_extract_file = overlap_variants_extract_file,
        person_extract_file = agd_overlap_person_extract_file,
        target_prefix = output_prefix_agd,
        target_gcp_folder = output_folder,
        chromosomes = chromosomes
    }

    call PLINK_pgendiff as PLINK_pgendiff {
      input:
        pgen_file_1 = ExtractSubset_AGD.output_pgen_file,
        pvar_file_1 = ExtractSubset_AGD.output_pvar_file,
        psam_file_1 = ExtractSubset_AGD.output_psam_file,
        pgen_file_2 = ExtractSubset_MEGA.output_pgen_file,
        pvar_file_2 = ExtractSubset_MEGA.output_pvar_file,
        psam_file_2 = ExtractSubset_MEGA.output_psam_file,
        update_ids1 = update_ids_agd,
        update_ids2 = update_ids_mega,
        target_prefix_agd = output_prefix_agd,  
        target_prefix_mega = output_prefix_mega,
        target_gcp_folder = output_folder
    }
  
      if(defined(output_folder)){
    call http_GcpUtils.MoveOrCopyThreeFiles as CopyFiles_descriptives1 {
      input:
        source_file1 = PLINK_pgendiff.output_freq_file_1,
        source_file2 = PLINK_pgendiff.output_geno_miss_file_1,
        source_file3 = PLINK_pgendiff.output_person_miss_file_1,
        is_move_file = false,
        target_gcp_folder = select_first([output_folder])
    }
  }

        if(defined(output_folder)){
    call http_GcpUtils.MoveOrCopyThreeFiles as CopyFiles_descriptives2 {
      input:
        source_file1 = PLINK_pgendiff.output_freq_file_2,
        source_file2 = PLINK_pgendiff.output_geno_miss_file_2,
        source_file3 = PLINK_pgendiff.output_person_miss_file_2,
        is_move_file = false,
        target_gcp_folder = select_first([output_folder])
    }
  }

    if(defined(output_folder)){
    call http_GcpUtils.MoveOrCopyTwoFiles as CopyFiles_pgendiff {
      input:
        source_file1 = PLINK_pgendiff.output_plink_pgendiff,
        source_file2 = PLINK_pgendiff.output_plink_log,
        is_move_file = false,
        target_gcp_folder = select_first([output_folder])
    }
  }

  output {
    File output_pgen_file = select_first([CopyFiles_pgendiff.output_file1, PLINK_pgendiff.output_plink_pgendiff])
    File output_pvar_file = select_first([CopyFiles_pgendiff.output_file2, PLINK_pgendiff.output_plink_log])
    
    File output_freq_file_1 = select_first([CopyFiles_descriptives1.output_file1, PLINK_pgendiff.output_freq_file_1])
    File output_geno_miss_file_1 = select_first([CopyFiles_descriptives1.output_file2, PLINK_pgendiff.output_geno_miss_file_1])
    File output_person_miss_file_1 = select_first([CopyFiles_descriptives1.output_file3, PLINK_pgendiff.output_person_miss_file_1])
    
    File output_freq_file_2 = select_first([CopyFiles_descriptives2.output_file1, PLINK_pgendiff.output_freq_file_2])
    File output_geno_miss_file_2 = select_first([CopyFiles_descriptives2.output_file2, PLINK_pgendiff.output_geno_miss_file_2])
    File output_person_miss_file_2 = select_first([CopyFiles_descriptives2.output_file3, PLINK_pgendiff.output_person_miss_file_2])
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

    String target_prefix_agd
    String target_prefix_mega
    String target_gcp_folder

    Int memory_gb = 20

    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
  }

  Int disk_size = ceil(size([pgen_file_1, psam_file_1, pvar_file_1, pgen_file_2, psam_file_2, pvar_file_2], "GB")  * 3) + 20

  String freq1 = target_prefix_agd + "_.afreq"
  String genomiss1 = target_prefix_agd + "_.vmiss"
  String personmiss1 = target_prefix_agd + "_.smiss"
  String freq1_suffix = target_prefix_agd + "_freq.txt"
  String genomiss1_suffix = target_prefix_agd + "_geno_miss.txt"
  String personmiss1_suffix = target_prefix_agd + "_person_miss.txt"

  String freq2 = target_prefix_mega + "_.afreq"
  String genomiss2 = target_prefix_mega + "_.vmiss"
  String personmiss2 = target_prefix_mega + "_.smiss"
  String freq2_suffix = target_prefix_mega + "_freq.txt"
  String genomiss2_suffix = target_prefix_mega + "_geno_miss.txt"
  String personmiss2_suffix = target_prefix_mega + "_person_miss.txt"

  String output_plink_pgendiff = target_prefix_agd + "_pdiff"
  String output_plink_log = target_prefix_agd + "_pdiff.log"

## FIDs need to be changed to GRIDs. Remove duplicate variants in MEGA.

  command {
  
      plink2 \
      --pgen ~{pgen_file_1} \
      --pvar ~{pvar_file_1} \
      --psam ~{psam_file_1} \
      --update-ids ~{update_ids1} \
      --rm-dup force-first \
      --maf 0.01 \
      --geno 0.01 \
      --missing \
      --freq \
      --make-pgen \
      --out intermediate1 
      
      plink2 \
      --pfile intermediate1 \
      --missing \
      --freq

    mv ~{freq1} ~{freq1_suffix}
    mv ~{genomiss1} ~{genomiss1_suffix}
    mv ~{personmiss1} ~{personmiss1_suffix}
      
      plink2 \
      --pgen ~{pgen_file_2} \
      --pvar ~{pvar_file_2} \
      --psam ~{psam_file_2} \
      --update-ids ~{update_ids2} \
      --rm-dup force-first \
      --maf 0.01 \
      --geno 0.01 \
      --make-pgen \
      --out intermediate2

      plink2 \
      --pfile intermediate2 \
      --missing \
      --freq

    mv ~{freq2} ~{freq2_suffix}
    mv ~{genomiss2} ~{genomiss2_suffix}
    mv ~{personmiss2} ~{personmiss2_suffix}
      
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

    File output_freq_file_1 = freq1_suffix
    File output_geno_miss_file_1 = genomiss1_suffix
    File output_person_miss_file_1 = personmiss1_suffix

    File output_freq_file_2 = freq2_suffix
    File output_geno_miss_file_2 = genomiss2_suffix
    File output_person_miss_file_2 = personmiss2_suffix

  }

}
