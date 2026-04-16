#include <iostream>
#include <fstream>
#include <iomanip>
#include <map>
#include <string>
#include "TFile.h"
#include "TTree.h"
#include "TApplication.h"

void DO_FILE(string filename, string filetag);

void MakeFilterEff(string listname, string filetag){
  string line;
  ifstream ifile(listname.c_str());

  if(ifile.is_open()){
    while(getline(ifile,line)){
      DO_FILE(line, filetag);
    }
    ifile.close();
  }
  gApplication->Terminate(0);
}

void DO_FILE(string filename, string filetag){

  string dataset = filename;
  int lumiblock;
  double efficiency;

  size_t found;
  while(dataset.find("/") != std::string::npos){
    found = dataset.find("/");
    dataset.erase(0,found+1);
  }
  while(dataset.find(".") != std::string::npos){
    found = dataset.find(".");
    dataset.erase(found);
  }

  TFile* fout = new TFile((dataset+"_"+filetag+".root").c_str(),"RECREATE");
  TTree* tout = (TTree*) new TTree("FilterEff", "FilterEff");
  
  tout->Branch("efficiency", &efficiency);
  tout->Branch("lumiblock", &lumiblock);
  tout->Branch("filetag", &filetag);
  tout->Branch("dataset", &dataset);

  string line;
  ifstream ifile(filename.c_str());

std::cout << "input: " << filename << std::endl;
  int dum[3];
  char eff[100];
  if(ifile.is_open()){
int written = 0;
    while(getline(ifile,line)){
      //sscanf(line.c_str(),"%d,%s,%d,%d,%d", &lumiblock, eff, &dum[0],&dum[1],&dum[2] );
      //efficiency = std::atof(eff);
      if (!line.empty() && line.back() == '\r') line.pop_back();
      int n = sscanf(line.c_str(), "%d,%lf,%d,%d,%d",
               &lumiblock, &efficiency,
               &dum[0], &dum[1], &dum[2]);
      if (n != 5) {
          std::cout << "Bad line: " << line << std::endl;
          continue;
      }
      if(efficiency < 0.)
	efficiency = 1.;
      tout->Fill();
written++; 
    }
std::cout << "written: " << written << std::endl;
    ifile.close();
  }

  fout->cd();
  tout->Write("",TObject::kOverwrite);
  fout->Close();
}
