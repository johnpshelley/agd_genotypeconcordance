version 1.0

#IMPORTS
## According to this: https://cromwell.readthedocs.io/en/stable/Imports/ we can import raw from github
## so we can make use of the already written WDLs provided by WARP/VUMC Biostatistics

import "https://raw.githubusercontent.com/shengqh/warp/develop/tasks/vumc_biostatistics/GcpUtils.wdl" as http_GcpUtils
import "https://raw.githubusercontent.com/shengqh/warp/develop/pipelines/vumc_biostatistics/genotype/Utils.wdl" as http_GenotypeUtils
import "https://raw.githubusercontent.com/shengqh/warp/develop/pipelines/vumc_biostatistics/agd/AgdUtils.wdl" as http_AgdUtils

workflow ExtractSubset {
  input {
    File bed_file
    File bim_file
    File fam_file

    File id_map_file
    
    File fasta_file

    File variants_extract_file
    File person_extract_file

    String target_prefix
    String target_gcp_folder
  }

    call ExtractVariants as ExtractVariants{
      input:
        bed_file = bed_file,
        bim_file = bim_file,
        fam_file = fam_file,
        fasta_file = fasta_file,
        variants_extract_file = variants_extract_file,
        person_extract_file = person_extract_file,
        target_prefix = target_prefix
    }
  
    if(defined(target_gcp_folder)){
    call http_GcpUtils.MoveOrCopyThreeFiles as CopyFiles_plink2 {
      input:
        source_file1 = ExtractVariants.output_pgen_file,
        source_file2 = ExtractVariants.output_pvar_file,
        source_file3 = ExtractVariants.output_psam_file,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_pgen_file = select_first([CopyFiles_plink2.output_file1, ExtractVariants.output_pgen_file])
    File output_pvar_file = select_first([CopyFiles_plink2.output_file2, ExtractVariants.output_pvar_file])
    File output_psam_file = select_first([CopyFiles_plink2.output_file3, ExtractVariants.output_psam_file])
  }
}

### TASK DEFINITIONS 

task ExtractVariants{
  input {
    File bed_file
    File bim_file
    File fam_file 
    File fasta_file

    File variants_extract_file
    File person_extract_file

    String target_prefix

    Int memory_gb = 20

    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
  }

  Int disk_size = ceil(size([bed_file, fam_file, bim_file], "GB")  * 2) + 20

  String intermediate_pgen = "intermediate.pgen"
  String intermediate_pvar = "intermediate.pvar"
  String intermediate_psam = "intermediate.psam"

  String new_pgen = target_prefix + ".pgen"
  String new_pvar = target_prefix + ".pvar"
  String new_psam = target_prefix + ".psam"

  command {  
    plink2 \
      --bed ~{bed_file} \
      --bim ~{bim_file} \
      --fam ~{fam_file} \
      --fa ~{fasta_file} \
      --ref-from-fa force \
      --chr 1-22 \
      --make-pgen \
      --out intermediate
    
    plink2 \
      --pgen ~{intermediate_pgen} \
      --pvar ~{intermediate_pvar} \
      --psam ~{intermediate_psam} \
      --extract range ~{variants_extract_file} \
      --keep ~{person_extract_file} \
      --max-alleles 2 \
      --set-all-var-ids @:#:\$r:\$a \
      --new-id-max-allele-len 10000 \
      --make-pgen \
      --out ~{target_prefix}

  }

  runtime {
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }

  output {
    File output_pgen_file = new_pgen
    File output_pvar_file = new_pvar
    File output_psam_file = new_psam
  }

}