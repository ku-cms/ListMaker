#!/bin/sh
export SCRAM_ARCH=slc7_amd64_gcc700
source /cvmfs/cms.cern.ch/cmsset_default.sh

scramv1 project CMSSW CMSSW_10_6_4
cd CMSSW_10_6_4/src/
eval `scramv1 runtime -sh`
cd ${_CONDOR_SCRATCH_DIR}
echo "running with args: $1 $2"
cmsRun runGenFilterEfficiencyAnalyzer_cfg.py inputFiles="$1" > "$2"
echo "Wrote output to: $2"
