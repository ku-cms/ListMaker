import os, glob
from CondorJobCountMonitor import CondorJobCountMonitor

def make_submit_sh(srcfile,year,dataset):
    fsrc = open(srcfile,'w')
    fsrc.write('universe = vanilla \n') 
    fsrc.write('executable = execute_script.sh \n')
    fsrc.write('use_x509userproxy = true \n')
    fsrc.write('Arguments = $(Item) '+dataset+'_$(ProcId).txt \n')
    fsrc.write('output = $ENV(PWD)/condor_'+year+'/out/'+dataset+'/'+dataset+'_$(ProcId).out \n')
    fsrc.write('error = $ENV(PWD)/condor_'+year+'/err/'+dataset+'/'+dataset+'_$(ProcId).err \n')
    fsrc.write('log = $ENV(PWD)/condor_'+year+'/log/'+dataset+'/'+dataset+'_$(ProcId).log \n')
    fsrc.write('request_memory = 2 GB \n')
    fsrc.write('transfer_input_files = runGenFilterEfficiencyAnalyzer_cfg.py\n')
    fsrc.write('should_transfer_files = YES \n')
    fsrc.write('when_to_transfer_output = ON_EXIT \n')
    fsrc.write('transfer_output_files = '+dataset+'_$(ProcId).txt \n')
    fsrc.write('transfer_output_remaps = "'+dataset+'_$(ProcId).txt=$ENV(PWD)/condor_'+year+'/txt/'+dataset+'/'+dataset+'_$(ProcId).txt" \n')
    fsrc.write('+DesiredOS="SL7"\n')
    fsrc.write('queue $(Item) from '+path_to_MINI+year+'/'+dataset+'.txt \n')
    fsrc.close()

path_to_MINI = "../../../samples/MINI/"
dir_list = [os.path.basename(d) + "/" for d in glob.glob(path_to_MINI + "*X_SMS")]
dir_list = [idir for idir in dir_list if not "102X" in idir] # preUL minis are not on disk so we just use existing outputs
for directory in dir_list:
    files = [f for f in os.listdir(path_to_MINI+directory) if f.endswith(".txt")]
    os.system("ls "+path_to_MINI+directory+" > lists_"+directory.replace('/','')+".txt")
    os.system("rm -rf condor_"+directory)
    monitor = CondorJobCountMonitor(threshold=90000, verbose=False)
    for file in files:
        dataset = file.replace('.txt','')
        os.system("mkdir -p condor_"+directory+'src/')
        os.system("mkdir -p condor_"+directory+'out/'+dataset+'/')
        os.system("mkdir -p condor_"+directory+'err/'+dataset+'/')
        os.system("mkdir -p condor_"+directory+'log/'+dataset+'/')
        os.system("mkdir -p condor_"+directory+'txt/'+dataset+'/')
        srcfile = "condor_"+directory+"src/"+dataset+".submit"
        make_submit_sh(srcfile,directory.replace('/',''),dataset)
        monitor.wait_until_jobs_below()
        print("condor_submit "+srcfile)
        os.system("condor_submit "+srcfile)
    break # debug
