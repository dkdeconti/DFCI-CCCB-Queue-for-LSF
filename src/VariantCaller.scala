package org.broadinstitute.gatk.queue.qscripts

import org.broadinstitute.gatk.queue.QScript
import org.broadinstitute.gatk.queue.extensions.gatk._

class VariantCaller extends QScript {
  /**
    * Batch variant calls with HaplotypeCaller via scatter gather.
    * Currently works given lists intervals and multiple inputs of BAM files.
    */
  @Input(doc="The reference file for the BAM files.", shortName="R")
  var referenceFile: File = null

  // Multiple BAMs -> multiple -I bamFile
  @Input(doc="One or more BAM files.", shortName="I")
  var bamFiles: List[File] = Nil

  @Input(doc="Intervals to traverse.", shortName="L", required=false)
  var intervals: List[File] = Nil

  @Argument(doc="Maxmem.", shortName="mem", required=false)
  var maxMem: Int = 6 // default to 6G

  @Argument(doc="Number of cpu threads per data thread.", shortName="nct", required=false)
  var numCPUThreads: Int = 1 // Leave at 1 thread, especially if LSF

  @Argument(doc="Number of scatters.", shortName="nsc", required=true)
  var numScatters: Int = _

  @Output
  var out: File = _

  def script() {
    // Run HaplotypeCaller for all bams jointly. [Required]
    val jointVariantCaller = new HaplotypeCaller
    jointVariantCaller.reference_sequence = referenceFile
    jointVariantCaller.input_file = bamFiles
    jointVariantCaller.intervalsString = intervals

    // Add the function to the pipeline
    add(jointVariantCaller)

    // If there is more than one BAM, also run HaplotypeCaller once for each bam.
    if (bamFiles.size > 0) {
      for (bamFile <- bamFiles) {
        val outGVCF = swapExt(bamFile, ".sorted.bam", ".g.vcf")

        val singleVariantCaller = new HaplotypeCaller
        singleVariantCaller.reference_sequence = referenceFile
        singleVariantCaller.input_file :+= bamFile
        singleVariantCaller.intervalsString = intervals
        singleVariantCaller.out = outGVCF

        add(singleVariantCaller)
      }
    }
  }
}