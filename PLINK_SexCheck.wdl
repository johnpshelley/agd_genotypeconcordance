version 1.0

#IMPORTS
## According to this: https://cromwell.readthedocs.io/en/stable/Imports/ we can import raw from github
## so we can make use of the already written WDLs provided by WARP/VUMC Biostatistics

import "https://raw.githubusercontent.com/shengqh/warp/develop/tasks/vumc_biostatistics/GcpUtils.wdl" as http_GcpUtils
import "https://raw.githubusercontent.com/shengqh/warp/develop/pipelines/vumc_biostatistics/genotype/Utils.wdl" as http_GenotypeUtils
import "https://raw.githubusercontent.com/shengqh/warp/develop/pipelines/vumc_biostatistics/agd/AgdUtils.wdl" as http_AgdUtils

workflow CheckSex_plink {
  input {
    File bed_file
    File bim_file
    File fam_file
	File update_sex
    File person_extract_file
    String target_prefix
    String target_gcp_folder
  }

    call CheckSex_plink{
      input:
        bed_file = bed_file,
        bim_file = bim_file,
        fam_file = fam_file,
        update_sex = update_sex,
        person_extract_file = person_extract_file,
        target_prefix = target_prefix
    }
  
  if(defined(target_gcp_folder)){
    call http_GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = CheckSex_plink.output_sexcheck,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_sexcheck = select_first([CopyFile.output_file, CheckSex_plink.output_sexcheck])
  }
}

### TASK DEFINITIONS 

task CheckSex_plink{
  input {
    File bed_file
    File bim_file
    File fam_file 
    
    File update_sex
    File person_extract_file
    
    String target_prefix
    Int memory_gb = 20

    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
  }

  Int disk_size = ceil(size([bed_file, fam_file, bim_file], "GB")  * 2) + 20
  
  String intermediate_bed = "intermediate.bed"
  String intermediate_bim = "intermediate.bim"
  String intermediate_fam = "intermediate.fam"

  String output_sexcheck = target_prefix + ".sexcheck"

  command {    
    plink2 \
      --bim ~{bim_file} \
      --bed ~{bed_file} \
      --fam ~{fam_file} \
      --make-bed \
      --chr X,Y \
      --keep ~{person_extract_file} \
      --update-sex ~{update_sex} \
      --out intermediate
    
    plink \
      --bim ~{intermediate_bim} \
      --bed ~{intermediate_bed} \
      --fam ~{intermediate_fam} \
      --check-sex \
      --out ~{target_prefix}

  }

  runtime {
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }

  output {
    File output_sexcheck = output_sexcheck
  }

}
