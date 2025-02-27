import os
import fileinput
import sys
from optparse import OptionParser

#options
parser = OptionParser()
parser.add_option("-p", "--path", dest="directory", 
                  help="Specify input directory containing the .txt files.", metavar="PATH")

(options, args) = parser.parse_args()

directory = options.directory

if not directory:
    sys.exit("You need to specify the directory! (See help).")

txtfiles=[]
fastsim_txtfiles = []
fullsim_txtfiles = []

list_name = directory.split("/")[-2]
# loop over input files
for filename in os.listdir(directory):
    if filename.endswith(".txt"):
        txtfiles.append(filename)
    if 'SMS' in list_name:
        file_path = os.path.join(directory, filename)
        for line in fileinput.input(file_path, inplace=True):
            print(line, end='')  # Ensure lines are preserved
            if 'Fast' in line or 'FS' in line:
                fastsim_txtfiles.append(os.path.join("samples/NANO", list_name, filename))
            else:
                fullsim_txtfiles.append(os.path.join("samples/NANO", list_name, filename))

# make root text file list
if not os.path.isdir("samples/NANO/Lists/"):
    os.makedirs("samples/NANO/Lists/")
txtfiles.sort()
with open(("samples/NANO/Lists/"+list_name+".list"), 'w') as filehandle:
    for listitem in txtfiles:
        filehandle.write(f'samples/NANO/{list_name}/{listitem}\n')
if 'SMS' in list_name:
    fastsim_txtfiles = list(set(fastsim_txtfiles))
    fastsim_txtfiles.sort()
    with open(("samples/NANO/Lists/"+list_name+"_FastSim.list"), 'w') as filehandle:
        for listitem in fastsim_txtfiles:
            filehandle.write(f'samples/NANO/{list_name}/{listitem}\n')
    fullsim_txtfiles = list(set(fullsim_txtfiles))
    fullsim_txtfiles.sort()
    with open(("samples/NANO/Lists/"+list_name+"_FullSim.list"), 'w') as filehandle:
        for listitem in fullsim_txtfiles:
            filehandle.write(f'samples/NANO/{list_name}/{listitem}\n')
