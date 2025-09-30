#!/bin/sh
source /cvmfs/cms.cern.ch/cmsset_default.sh
# ENV for UL
if [ "$3" = "UL" ]; then
  export SCRAM_ARCH=slc7_amd64_gcc700
  scramv1 project CMSSW CMSSW_10_6_4
  cd CMSSW_10_6_4/src/
fi
# ENV for Run3
if [ "$3" = "Run3" ]; then
  export SCRAM_ARCH=el9_amd64_gcc12
  scramv1 project CMSSW CMSSW_13_3_1
  cd CMSSW_13_3_1/src/
fi
# Running filter code
eval `scramv1 runtime -sh`
cd "${_CONDOR_SCRATCH_DIR}"
echo "running with args: $1 $2"
cmsRun runGenFilterEfficiencyAnalyzer_cfg.py inputFiles="$1" > "$2"
echo "Wrote output to: $2"

