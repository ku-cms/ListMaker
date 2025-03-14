This repo needs to be inside of a CMSSW directory and most scripts require a grid cert to run

To make lists of files for bkg datasets run:

nohup bash -c "time python3 batchList.py -i DataSetsList/bkg/" > batchList_bkg.debug 2>&1 &

To Make Filter Eff Files:

Use batchlist to make lists of MINIAOD files for SMS samples

python3 batchList.py -i DataSetsList/sms/ -o samples/ --mini
(ignore warnings)

Then go to: GeneratorInterface/Core/test/ and then run:

python3 make_filter_file.py

Finally run:

python3 convert_filter_file.py

For using XSDB Scraper: 

Need selenium to be installed after running cmsenv:
python3 -m pip install --user selenium

Need chrome-driver program for linux (example):
chromedriver-linux64/chromedriver

Need to install chrome locally:
wget https://dl.google.com/linux/direct/google-chrome-stable_current_x86_64.rpm
rpm2cpio google-chrome-stable_current_x86_64.rpm | cpio -idmv

Once selenium and chrome are installed (only need to do one time) open XSDB_HTML_Scraper.py and edit paths
Can search for YOUR_PATH
python3 XSDB_HTML_Scraper.py --idir DataSetsList/bkg/
