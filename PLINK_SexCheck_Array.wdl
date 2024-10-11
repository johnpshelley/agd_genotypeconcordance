version 1.0

#IMPORTS
## According to this: https://cromwell.readthedocs.io/en/stable/Imports/ we can import raw from github
## so we can make use of the already written WDLs provided by WARP/VUMC Biostatistics

import "https://raw.githubusercontent.com/shengqh/warp/develop/tasks/vumc_biostatistics/GcpUtils.wdl" as http_GcpUtils
import "https://raw.githubusercontent.com/shengqh/warp/develop/pipelines/vumc_biostatistics/genotype/Utils.wdl" as http_GenotypeUtils
import "https://raw.githubusercontent.com/shengqh/warp/develop/pipelines/vumc_biostatistics/agd/AgdUtils.wdl" as http_AgdUtils

workflow CheckSex_plink {
  input {
    Array[File] pgen_files
    Array[File] pvar_files
    Array[File] psam_files

    Array[String] chromosomes

	  File update_sex
    File person_extract_file
    File id_map_file

    String target_prefix
    String target_gcp_folder
  }

  scatter (idx in range(length(chromosomes))) {
    String chromosome = chromosomes[idx]
    File pgen_file = pgen_files[idx]
    File pvar_file = pvar_files[idx]
    File psam_file = psam_files[idx]
    String replaced_sample_name = "~{chromosome}.psam"

#I think I need this to get the IDs correctly as GRIDS

  call http_AgdUtils.ReplaceICAIdWithGrid as ReplaceICAIdWithGrid {
    input:
      input_psam = psam_file,
      id_map_file = id_map_file,
      output_psam = replaced_sample_name
  }

  call ExtractVariants as ExtractVariants{
    input:
        pgen_file = pgen_file,
        pvar_file = pvar_file,
        psam_file = ReplaceICAIdWithGrid.output_psam,
        chromosome = chromosome,
        person_extract_file = person_extract_file
    }
  }

  call MergePgenFiles_MOD as MergePgenFiles_MOD{
    input:
      pgen_files = ExtractVariants.output_pgen_file,
      pvar_files = ExtractVariants.output_pvar_file,
      psam_files = ExtractVariants.output_psam_file,
      target_prefix = target_prefix
  }

    call CheckSex_plink{
      input:
        pgen_file = MergePgenFiles_MOD.output_pgen_file,
        pvar_file = MergePgenFiles_MOD.output_pvar_file,
        psam_file = MergePgenFiles_MOD.output_psam_file,
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

task ExtractVariants{
  input {
    File pgen_file
    File pvar_file
    File psam_file 
    String chromosome
    File person_extract_file

    Int memory_gb = 40
    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
  }

  Int disk_size = ceil(size([pgen_file, psam_file, pvar_file], "GB")  * 2) + 40

  String new_pgen = chromosome + ".pgen"
  String new_pvar = chromosome + ".pvar"
  String new_psam = chromosome + ".psam"

  command {
    plink2 \
      --pgen ~{pgen_file} \
      --pvar ~{pvar_file} \
      --psam ~{psam_file} \
      --make-pgen \
      --out ~{chromosome}
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

task MergePgenFiles_MOD {
  input {
    Array[File] pgen_files
    Array[File] pvar_files
    Array[File] psam_files
    
    String target_prefix

    Int memory_gb = 40

    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
  }

  Int disk_size = ceil((size(pgen_files, "GB") + size(pvar_files, "GB") + size(psam_files, "GB"))  * 3) + 40

  String new_pgen = target_prefix + ".pgen"
  String new_pvar = target_prefix + ".pvar"
  String new_psam = target_prefix + ".psam"

  String new_merged_pgen = target_prefix + "-merge.pgen"
  String new_merged_pvar = target_prefix + "-merge.pvar"
  String new_merged_psam = target_prefix + "-merge.psam"

  command <<<

cat ~{write_lines(pgen_files)} > pgen.list
cat ~{write_lines(pvar_files)} > pvar.list
cat ~{write_lines(psam_files)} > psam.list

paste pgen.list pvar.list psam.list > merge.list

plink2 --pmerge-list merge.list --make-pgen --out ~{target_prefix}

rm -f ~{new_pgen} ~{new_pvar} ~{new_psam}

mv ~{new_merged_pgen} ~{new_pgen}
mv ~{new_merged_pvar} ~{new_pvar}
mv ~{new_merged_psam} ~{new_psam}

>>>

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

task CheckSex_plink {
  input {
    File pgen_file
    File pvar_file
    File psam_file 
    
    File update_sex
    File person_extract_file
    
    String target_prefix
    Int memory_gb = 40

    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
  }

  Int disk_size = ceil(size([pgen_file, pvar_file, psam_file], "GB")  * 2) + 40
  
  String intermediate1_bed = "intermediate1.bed"
  String intermediate1_bim = "intermediate1.bim"
  String intermediate1_fam = "intermediate1.fam"
  String intermediate1_ldred = "intermediate1.prune.in"

  String intermediate2_bed = "intermediate2.bed"
  String intermediate2_bim = "intermediate2.bim"
  String intermediate2_fam = "intermediate2.fam"

  String output_sexcheck = target_prefix + ".sexcheck"

  command {    
    plink2 \
      --pgen ~{pgen_file} \
      --pvar ~{pvar_file} \
      --psam ~{psam_file} \
      --maf 0.01 \
      --max-alleles 2 \
      --split-par hg38 \
      --update-sex ~{update_sex} \
      --set-all-var-ids @:#:$r:$a \
      --indep-pairwise 20000 2000 0.5 \
      --make-bed \
      --out intermediate1

    plink2 \
      --bed ~{intermediate1_bed} \
      --bim ~{intermediate1_bim} \
      --fam ~{intermediate1_fam} \
      --keep ~{person_extract_file} \
      --extract ~{intermediate1_ldred} \
      --make-bed \
      --out intermediate2
    
    plink \
      --bed ~{intermediate2_bed} \
      --bim ~{intermediate2_bim} \
      --fam ~{intermediate2_fam} \
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
