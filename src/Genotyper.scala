package org.broadinstitute.gatk.queue.qscripts

import org.broadinstitute.gatk.queue.QScript
import org.broadinstitute.gatk.queue.extensions.gatk._

class Genotyper extends QScript {
  /**
    * Batch genotypes GVCF files via scatter gather.
    * Currently works given lists of gvcf and intervals.
    */
  @Input(doc="The reference file for the GVCF files.", shortName="R")
  var referenceFile: File = null

  @Input(doc="One or more GVCF files.", shortName="I")
  var gvcfFiles: List[File] = Nil

  @Input(doc="Intervals to traverse.", shortName="L", required=false)
  var intervals: List[File] = Nil

  @Argument(doc="Maxmem.", shortName="mem", required=false)
  var maxMem: Int = 6

  @Argument(doc="Number of cpu threads per data thread.", shortName="nct", required=false)
  var numCPUThreads: Int = 1

  @Argument(doc="Number of cpu threads", shortName="nt", required=false)
  var numThreads: Int = 1

  @Argument(doc="Number of scatters.", shortName="nsc", required=true)
  var numScatters: Int = _

  @Output
  var out: File = _


  def script() {
    // Initialize GenotypeGVCFs
    val genotypeGVCFs = new GenotypeGVCFs

    // Initialize genotypeGVCFs input
    genotypeGVCFs.reference_sequence = referenceFile
    genotypeGVCFs.variant = gvcfFiles
    genotypeGVCFs.intervalsString = intervals

    // Initialize genotypGVCFs output
    genotypeGVCFs.out = out

    // Initialize genotypeGVCFs arguments
    genotypeGVCFs.memoryLimit = maxMem
    genotypeGVCFs.scatterCount = numScatters
    genotypeGVCFs.num_cpu_threads_per_data_thread = numCPUThreads
    genotypeGVCFs.num_threads = numThreads

    // Start
    add(genotypeGVCFs)
  }
}