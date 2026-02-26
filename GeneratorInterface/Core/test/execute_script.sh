#!/bin/bash
set -e

source /cvmfs/cms.cern.ch/cmsset_default.sh

INPUTFILE="$1"
OUTPUTFILE="$2"
MODE="$3"

echo "========================================"
echo "Running job"
echo "Input  : $INPUTFILE"
echo "Output : $OUTPUTFILE"
echo "Mode   : $MODE"
echo "Scratch: ${_CONDOR_SCRATCH_DIR}"
echo "Host   : $(hostname)"
echo "========================================"

# Always run from scratch directory
cd "${_CONDOR_SCRATCH_DIR}"

########################################
# UL (Run2) — native SLC7
########################################
if [ "$MODE" = "UL" ]; then
  export SCRAM_ARCH=slc7_amd64_gcc700

  scramv1 project CMSSW CMSSW_10_6_4
  cd CMSSW_10_6_4/src
  eval "$(scramv1 runtime -sh)"

  cd "${_CONDOR_SCRATCH_DIR}"

  cmsRun runGenFilterEfficiencyAnalyzer_cfg.py inputFiles="$INPUTFILE" > "$OUTPUTFILE"
fi

########################################
# Run3 — force EL9 container
########################################
if [ "$MODE" = "Run3" ]; then
  cmssw-el9 -- /bin/bash << EOF
set -e
source /cvmfs/cms.cern.ch/cmsset_default.sh

export SCRAM_ARCH=el9_amd64_gcc12

scramv1 project CMSSW CMSSW_13_3_1
cd CMSSW_13_3_1/src
eval "\$(scramv1 runtime -sh)"

cd ${_CONDOR_SCRATCH_DIR}

echo "Inside container:"
echo "PWD = \$(pwd)"
echo "Running cmsRun..."

cmsRun runGenFilterEfficiencyAnalyzer_cfg.py inputFiles="$INPUTFILE" > "$OUTPUTFILE"

EOF
fi

echo "========================================"
echo "Finished job"
ls -lh
echo "Wrote output to: $OUTPUTFILE"
echo "========================================"
