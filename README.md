This repo needs to be inside of a CMSSW directory and most scripts require a grid cert to run

To make lists of files for datasets run:

nohup bash -c "time python3 batchList.py -i DataSetsList/bkg/" > batchList_bkg.debug 2>&1 &
nohup bash -c "time python3 batchList.py -i DataSetsList/sms/" > batchList_sms.debug 2>&1 &

To Make Filter Eff Files (run on LPC):

Use batchlist to make lists of MINIAOD files for SMS samples

nohup bash -c "time python3 batchList.py -i DataSetsList/sms/ --mini" > batchList_mini.debug 2>&1 &

Then go to: GeneratorInterface/Core/test/ and then run:

python3 make_filter_file.py

Then check jobs with:

python3 checkJobs.py

Once jobs are good to go, run to convert into format needed for ntuples:

python3 convert_filter_file.py

For using XSDB Scraper: 

Need added python packages to be installed after running cmsenv:
python3 -m pip install --user selenium deepdiff tqdm bs4

Need chrome-driver program for linux (example):
chromedriver-linux64/chromedriver

Need to install chrome locally:
cd ~
wget https://dl.google.com/linux/direct/google-chrome-stable_current_x86_64.rpm
rpm2cpio google-chrome-stable_current_x86_64.rpm | cpio -idmv

Once selenium and chrome are installed (only need to do one time) open XSDB_HTML_Scraper.py and edit paths
Can search for YOUR_PATH
Once editied to your paths, run:
python3 XSDB_HTML_Scraper.py --idir DataSetsList/bkg/
