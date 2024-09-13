version 1.0

#IMPORTS
## According to this: https://cromwell.readthedocs.io/en/stable/Imports/ we can import raw from github
## so we can make use of the already written WDLs provided by WARP/VUMC Biostatistics

import "https://raw.githubusercontent.com/shengqh/warp/develop/tasks/vumc_biostatistics/GcpUtils.wdl" as http_GcpUtils
import "https://raw.githubusercontent.com/shengqh/warp/develop/pipelines/vumc_biostatistics/genotype/Utils.wdl" as http_GenotypeUtils
import "https://raw.githubusercontent.com/shengqh/warp/develop/pipelines/vumc_biostatistics/agd/AgdUtils.wdl" as http_AgdUtils

workflow ExtractSubset {
  input {
    Array[File] pgen_files
    Array[File] pvar_files
    Array[File] psam_files

    Array[String] chromosomes

    String target_prefix

    File id_map_file
    
    File fasta_file

    File variants_extract_file
    File person_extract_file

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
        fasta_file = fasta_file,
        chromosome = chromosome,
        variants_extract_file = variants_extract_file,
        person_extract_file = person_extract_file
    }
  }

  call MergePgenFiles_MOD as MergePgenFiles_MOD{
    input:
      pgen_files = ExtractVariants.output_pgen_file,
      pvar_files = ExtractVariants.output_pvar_file,
      psam_files = ExtractVariants.output_psam_file,
      freq_files = ExtractVariants.output_freq,
      geno_miss_files = ExtractVariants.output_geno_miss,
      person_miss_files = ExtractVariants.output_person_miss,
      target_prefix = target_prefix
  }
  
    if(defined(target_gcp_folder)){
    call http_GcpUtils.MoveOrCopyThreeFiles as CopyFiles_plink2 {
      input:
        source_file1 = MergePgenFiles_MOD.output_pgen_file,
        source_file2 = MergePgenFiles_MOD.output_pvar_file,
        source_file3 = MergePgenFiles_MOD.output_psam_file,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }
  
      if(defined(target_gcp_folder)){
    call http_GcpUtils.MoveOrCopyThreeFiles as CopyFiles_descriptives {
      input:
        source_file1 = MergePgenFiles_MOD.output_freq_file,
        source_file2 = MergePgenFiles_MOD.output_geno_miss_file,
        source_file3 = MergePgenFiles_MOD.output_person_miss_file,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_pgen_file = select_first([CopyFiles_plink2.output_file1, MergePgenFiles_MOD.output_pgen_file])
    File output_pvar_file = select_first([CopyFiles_plink2.output_file2, MergePgenFiles_MOD.output_pvar_file])
    File output_psam_file = select_first([CopyFiles_plink2.output_file3, MergePgenFiles_MOD.output_psam_file])
    File output_freq_file = select_first([CopyFiles_descriptives.output_file1, MergePgenFiles_MOD.output_freq_file])
    File output_geno_miss_file = select_first([CopyFiles_descriptives.output_file2, MergePgenFiles_MOD.output_geno_miss_file])
    File output_person_miss_file = select_first([CopyFiles_descriptives.output_file3, MergePgenFiles_MOD.output_person_miss_file])
  }
}

### TASK DEFINITIONS 

task ExtractVariants{
  input {
    File pgen_file
    File pvar_file
    File psam_file 
    File fasta_file

    String chromosome

    File variants_extract_file
    File person_extract_file

    Int memory_gb = 20

    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
  }

  Int disk_size = ceil(size([pgen_file, psam_file, pvar_file], "GB")  * 2) + 20

  String intermediate_pgen = chromosome + "_intermediate.pgen"
  String intermediate_pvar = chromosome + "_intermediate.pvar"
  String intermediate_psam = chromosome + "_intermediate.psam"

  String new_pgen = chromosome + ".pgen"
  String new_pvar = chromosome + ".pvar"
  String new_psam = chromosome + ".psam"
  String new_afreq = chromosome + ".afreq"
  String new_geno_miss = chromosome + ".vmiss"
  String new_person_miss = chromosome + ".smiss"

  command {
    plink2 \
      --pgen ~{pgen_file} \
      --pvar ~{pvar_file} \
      --psam ~{psam_file} \
      --fa ~{fasta_file} \
      --ref-from-fa force \
      --chr 1-22 \
      --make-pgen \
      --out ~{chromosome}_intermediate
    
    plink2 \
      --pgen ~{intermediate_pgen} \
      --pvar ~{intermediate_pvar} \
      --psam ~{intermediate_psam} \
      --extract range ~{variants_extract_file} \
      --keep ~{person_extract_file} \
      --max-alleles 2 \
      --set-all-var-ids @:#:\$r:\$a \
      --new-id-max-allele-len 10000 \
      --missing \
      --freq \
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
    File output_freq = new_afreq
    File output_geno_miss = new_geno_miss
    File output_person_miss = new_person_miss
  }

}

task MergePgenFiles_MOD {
  input {
    Array[File] pgen_files
    Array[File] pvar_files
    Array[File] psam_files
    Array[File] freq_files
    Array[File] geno_miss_files
    Array[File] person_miss_files
    
    String target_prefix

    Int memory_gb = 20

    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
  }

  Int disk_size = ceil((size(pgen_files, "GB") + size(pvar_files, "GB") + size(psam_files, "GB"))  * 3) + 20

  String new_pgen = target_prefix + ".pgen"
  String new_pvar = target_prefix + ".pvar"
  String new_psam = target_prefix + ".psam"
  
  String new_freq = target_prefix + "_freq.txt"
  String new_geno_miss = target_prefix + "_geno_miss.txt"
  String new_person_miss = target_prefix + "_person_miss.txt"

  String new_merged_pgen = target_prefix + "-merge.pgen"
  String new_merged_pvar = target_prefix + "-merge.pvar"
  String new_merged_psam = target_prefix + "-merge.psam"

  command <<<

cat ~{write_lines(pgen_files)} > pgen.list
cat ~{write_lines(pvar_files)} > pvar.list
cat ~{write_lines(psam_files)} > psam.list

paste pgen.list pvar.list psam.list > merge.list

plink2 --pmerge-list merge.list --make-pgen --out ~{target_prefix}

rm -f ~{new_pgen} ~{new_pvar} ~{new_psam} ~{new_freq} ~{new_geno_miss} ~{new_person_miss}

mv ~{new_merged_pgen} ~{new_pgen}
mv ~{new_merged_pvar} ~{new_pvar}
mv ~{new_merged_psam} ~{new_psam}

cat ~{sep=' ' freq_files} > ~{new_freq}
cat ~{sep=' ' geno_miss_files} > ~{new_geno_miss}
cat ~{sep=' ' person_miss_files} > ~{new_person_miss}

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
    File output_freq_file = new_freq
    File output_geno_miss_file = new_geno_miss
    File output_person_miss_file = new_person_miss
  }
}