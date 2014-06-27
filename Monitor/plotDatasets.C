//==================================================================================================
//
// Plot dataset characteristics as reported in the local popularity database cache.
//
//
//==================================================================================================
#include <iostream>
#include <fstream>

#include <TROOT.h>
#include <TSystem.h>
#include <TStyle.h>
#include <TString.h>
#include <TCanvas.h>
#include <TGraph.h>
#include <TMultiGraph.h>
#include <TH1D.h>
#include <TLegend.h>
#include <TText.h>
using namespace std;

void plotDatasetUsage();

//--------------------------------------------------------------------------------------------------
void plotDatasets()
{
  TString  styleMacro = gSystem->Getenv("MIT_ROOT_STYLE");
  long int rc = gROOT->LoadMacro(styleMacro+"+");
  printf(" Return code of loading styles: %d\n",rc);

  plotDatasetUsage();
}

//--------------------------------------------------------------------------------------------------
void plotDatasetUsage()
{
  TString text        = gSystem->Getenv("DATASET_MONITOR_TEXT");
  TString fileName    = gSystem->Getenv("DATASET_MONITOR_FILE");

  TString pngFileName = fileName + TString(".png");    
  TString inputFile   = fileName + TString(".txt");

  // Make sure we have the right styles
  MitRootStyle::Init();

  // Now open our database output
  ifstream input;
  input.open(inputFile.Data());

  Int_t    nLines=0;
  Double_t totalSize=0;
  Double_t xMin=0, xMax=40;

  Int_t    nSites=0, nFiles=0, nAccesses=0;
  Double_t size=0;

  // book our histogram
  Int_t nBins = int(xMax-xMin)+1;
  TH1D *h = new TH1D("dataUsage","Data Usage",nBins,xMin-0.5,xMax+0.5);
  MitRootStyle::InitHist(h,"","",kBlack);  
  TString titles = TString("; Accesses ") + text + TString(";Data Size [TB]");
  h->SetTitle(titles.Data());

  // Loop over the file
  //-------------------
  while (1) {

    // read in 
    input >> nSites >> nAccesses >> nFiles >> size;

    // check it worked
    if (! input.good())
      break;
    
    // show what we are reading
    if (nLines < 5)
      printf(" nSites=%d  nAccesses=%d  nFiles=%d  size=%8f\n",nSites, nAccesses, nFiles, size);

    Double_t value = double(nAccesses)/double(nFiles*nSites), weight = double(nSites)*size/1024.;

    // treat the cases of few accesses:
    //
    // - to avoid confusing counts below zero pretend that accessing half of the files of
    //   one replica counts as one full access
    if (value<1.0 && (double(nAccesses)/double(nFiles)>0.5))
      value = 1.;

    // truncate at maximum (overflow in last bin) 
    if (value>=xMax)
      value = xMax-0.0001;
    
    // fill the histogram here
    h->Fill(value,weight);

    if (nLines < 100)
      printf(" Value: %f weight: %f\n",value,weight);
    
    // keep track of the total size
    totalSize += double(nSites)*size/1024.;

    // count the number of lines
    nLines++;
  }
  input.close();

  printf(" \n");
  printf(" Found %d entries.\n",nLines);
  printf(" Found %.3f [PB] total volume.\n",totalSize/1024.);
  printf(" \n");

  // Open a canvas
  TCanvas *cv = new TCanvas();
  cv->Draw();
  h->Draw("hist");

  MitRootStyle::OverlayFrame();
  MitRootStyle::AddText("Overflow in last bin.");

  cv->SaveAs(pngFileName.Data());
}
